package os.failsafe.executor;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.TestcontainersDbExtension;

import javax.sql.DataSource;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FailsafeExecutorTest {

    protected final static boolean IS_DEBUG = ManagementFactory.getRuntimeMXBean().getInputArguments().toString().contains("-agentlib:jdwp");

    @RegisterExtension
    static final TestcontainersDbExtension DB_EXTENSION = new TestcontainersDbExtension();

    private static DataSource dataSource;
    private FailsafeExecutor failsafeExecutor;
    private LocalDateTime now;
    private TestSystemClock systemClock;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = DB_EXTENSION.dataSource();

        now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        systemClock = new TestSystemClock();
        systemClock.fixedTime(now);

        failsafeExecutor = new FailsafeExecutor(
                systemClock,
                dataSource,
                2, // worker threads
                10, // queue size
                Duration.ZERO,
                Duration.ofMillis(100),
                Duration.ofSeconds(6),
                Duration.ofMillis(300),
                "test-node"
        );

        failsafeExecutor.registerErrorHandler(Throwable::printStackTrace);
    }

    @AfterEach
    void tearDown() {
        failsafeExecutor.shutdown();
    }

    @Test
    void task_is_executed_and_listener_called() throws Exception {
        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        failsafeExecutor.registerTask(taskName, (String p) -> executedCount.incrementAndGet());

        // Insert task manually (simulating enqueue)
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, taskName, null);
        }

        // Start queue and wait for execution
        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 1);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:"));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("success:"));
        assertTrue(failsafeExecutor.isRunning());
        assertTrue(failsafeExecutor.findOne(id).isEmpty());
    }

    @Test
    void transactional_task_is_executed_and_listener_called() throws Exception {
        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        failsafeExecutor.registerTask(taskName, (conn, param) -> {
            executedCount.incrementAndGet();
            assertFalse(conn.getAutoCommit());
        });

        // Insert task manually (simulating enqueue)
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, taskName, null);
        }

        // Start queue and wait for execution
        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 1);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:"));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("success:"));
        assertTrue(failsafeExecutor.isRunning());
        assertTrue(failsafeExecutor.findOne(id).isEmpty());
    }

    @Test
    void task_failure_marks_failed_and_can_retry() throws Exception {
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        var shouldFail = new AtomicBoolean(true);
        String failingTask = "failingTask";
        failsafeExecutor.registerTask(failingTask, (String p) -> {
            if (shouldFail.get()) {
                throw new RuntimeException("boom!");
            }
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, failingTask, null);
        }

        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.failedNotified);

        // Ensure fail columns set
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT FAIL_TIME, EXCEPTION_MESSAGE, LOCK_TIME, NODE_ID FROM FAILSAFE_TASK WHERE ID=?")) {
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp("FAIL_TIME"));
            assertNotNull(rs.getTimestamp("LOCK_TIME"));
            assertNotNull(rs.getString("NODE_ID"));
            assertEquals("boom!", rs.getString("EXCEPTION_MESSAGE"));
        }

        FailsafeExecutor.Task task = failsafeExecutor.findOne(id).get();
        assertNotNull(task.executionFailure().failTime());
        assertEquals("boom!", task.executionFailure().exceptionMessage());

        failsafeExecutor.shutdown();

        // Retry should reset fail fields
        try (Connection conn = dataSource.getConnection()) {
            boolean retried = failsafeExecutor.retry(conn, task);
            assertTrue(retried, "Retry should succeed");
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT FAIL_TIME, RETRY_COUNT, LOCK_TIME, NODE_ID, PLANNED_EXECUTION_TIME FROM FAILSAFE_TASK WHERE ID=?")) {
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getTimestamp("FAIL_TIME"));
            assertNull(rs.getTimestamp("LOCK_TIME"));
            assertNull(rs.getString("NODE_ID"));
            assertEquals(1, rs.getInt("RETRY_COUNT"));
            assertEquals(now, rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime());
        }

        shouldFail.set(false);
        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.succeededNotified);
    }

    @Test
    void schedule_creates_task_and_reschedules_on_completion() throws Exception {
        AtomicInteger runs = new AtomicInteger(0);

        FailsafeExecutor.Schedule everySecond = base ->
                Optional.of(base.plusSeconds(1)); // every second

        failsafeExecutor.schedule("scheduledTask", everySecond, runs::incrementAndGet);
        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofSeconds(1));

        // Wait for first execution
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> runs.get() >= 1);

        systemClock.timeTravelBy(Duration.ofSeconds(1));

        // Wait for a reschedule (at least two runs)
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> runs.get() >= 2);

        assertTrue(runs.get() >= 2, "Task should have rescheduled and executed at least twice");
    }

    @Test
    void start_and_shutdown_change_running_state() {
        assertFalse(failsafeExecutor.isRunning());
        failsafeExecutor.start();
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(failsafeExecutor::isRunning);
        failsafeExecutor.shutdown();
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> !failsafeExecutor.isRunning());
        assertFalse(failsafeExecutor.isRunning());
    }

    @Test
    void start_time_for_active_tasks_is_set() throws Exception {
        AtomicBoolean taskStarted = new AtomicBoolean(false);
        AtomicBoolean taskFinished = new AtomicBoolean(false);

        String taskName = "longRunningTask";
        String taskId = UUID.randomUUID().toString();

        // Register a task that blocks until we allow it to finish
        failsafeExecutor.registerTask(taskName, (String p) -> {
            taskStarted.set(true);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilTrue(taskFinished); // block until test signals
        });

        // Insert the task into DB
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, taskId, taskName, null);
        }

        assertNull(failsafeExecutor.findOne(taskId).get().startTime());

        // Start the queue
        failsafeExecutor.start();

        // Wait until the task starts running
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilTrue(taskStarted);

        assertTrue(failsafeExecutor.activeTaskIds.contains(taskId));
        LocalDateTime startTime = failsafeExecutor.findOne(taskId).get().startTime();
        assertNotNull(startTime, "START_TIME should be set when task starts execution");
        assertEquals(now, startTime);

        // Signal task to finish
        taskFinished.set(true);

        // Await task completion
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> !failsafeExecutor.activeTaskIds.contains(taskId));
    }

    @Test
    void heartbeat_extends_lock_time_for_active_tasks() throws Exception {
        AtomicBoolean taskStarted = new AtomicBoolean(false);
        AtomicBoolean taskFinished = new AtomicBoolean(false);

        String taskName = "longRunningTask";
        String taskId = UUID.randomUUID().toString();

        // Register a task that blocks until we allow it to finish
        failsafeExecutor.registerTask(taskName, (String p) -> {
            taskStarted.set(true);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilTrue(taskFinished); // block until test signals
        });

        // Insert the task into DB
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, taskId, taskName, null);
        }

        // Start the queue
        failsafeExecutor.start();

        // Wait until the task starts running
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilTrue(taskStarted);

        assertTrue(failsafeExecutor.activeTaskIds.contains(taskId));

        // Capture initial LOCK_TIME
        Timestamp initialLockTime;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT LOCK_TIME FROM FAILSAFE_TASK WHERE ID=?")) {
            ps.setString(1, taskId);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            initialLockTime = rs.getTimestamp(1);
            assertNotNull(initialLockTime, "LOCK_TIME should be set when task starts");
            assertEquals(now, initialLockTime.toLocalDateTime());
        }

        // time travel to first lock timeout
        systemClock.timeTravelBy(failsafeExecutor.heartbeatInterval);

        // Wait for heartbeat to extend LOCK_TIME
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    try (Connection conn = dataSource.getConnection();
                         PreparedStatement ps = conn.prepareStatement("SELECT LOCK_TIME FROM FAILSAFE_TASK WHERE ID=?")) {
                        ps.setString(1, taskId);
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()) {
                            Timestamp lockTime = rs.getTimestamp(1);
                            return lockTime != null && lockTime.after(initialLockTime) && systemClock.now().equals(lockTime.toLocalDateTime());
                        }
                        return false;
                    }
                });

        // Signal task to finish
        taskFinished.set(true);

        // Await task completion
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> !failsafeExecutor.activeTaskIds.contains(taskId));
    }

    @Test
    void cancel_failed_task() throws SQLException, IOException {
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        String failingTask = "failingTask";
        failsafeExecutor.registerTask(failingTask, (String p) -> {
            throw new RuntimeException("boom!");
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, failingTask, null);
        }

        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.failedNotified);

        FailsafeExecutor.Task task = failsafeExecutor.findOne(id).get();
        failsafeExecutor.cancel(task);

        assertTrue(failsafeExecutor.findOne(id).isEmpty());
    }

    @Test
    void remote_task_should_be_registered_but_never_executed() throws Exception {
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        AtomicBoolean executed = new AtomicBoolean(false);
        failsafeExecutor.registerRemoteTask("remoteTask");

        // Add both tasks into the DB
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, "2", "remoteTask", null, LocalDateTime.now());
        }

        // Start the queue
        failsafeExecutor.start();

        Thread.sleep(300);

        var remoteTask = failsafeExecutor.findOne("2").get();

        assertEquals("remoteTask", remoteTask.name());
        assertNull(remoteTask.lockTime());
        assertNull(remoteTask.executionFailure());
        assertNull(remoteTask.nodeId());
        assertTrue(taskExecutionListener.persistingNotified.get());
        assertFalse(taskExecutionListener.succeededNotified.get());
        assertFalse(taskExecutionListener.failedNotified.get());
        assertEquals(1, taskExecutionListener.listenerOrder.size());
    }

    @Test
    void concurrent_task_execution_with_multiple_worker_threads() throws Exception {
        // Create a latch to track concurrent execution
        var startLatch = new CountDownLatch(1);
        var executionLatch = new CountDownLatch(2);
        var concurrentExecutionTracker = new AtomicInteger(0);
        var maxConcurrentExecution = new AtomicInteger(0);

        // Register a task that will block until signaled
        failsafeExecutor.registerTask("concurrentTask", (String p) -> {
            try {
                // Wait for signal to start
                startLatch.await();

                // Track concurrent execution
                int current = concurrentExecutionTracker.incrementAndGet();
                int max = maxConcurrentExecution.get();
                while (current > max) {
                    maxConcurrentExecution.compareAndSet(max, current);
                    max = maxConcurrentExecution.get();
                }

                // Simulate work
                Thread.sleep(500);

                // Decrement counter and signal completion
                concurrentExecutionTracker.decrementAndGet();
                executionLatch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Add multiple tasks to the queue
        for (int i = 0; i < 2; i++) {
            String id = UUID.randomUUID().toString();
            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, id, "concurrentTask", null);
            }
        }

        failsafeExecutor.start();

        // Allow tasks to start executing
        startLatch.countDown();

        // Wait for both tasks to complete
        executionLatch.await(10, TimeUnit.SECONDS);

        // Verify that tasks executed concurrently
        assertEquals(2, maxConcurrentExecution.get(),
                "Both tasks should have executed concurrently");
    }

    @Test
    void queue_size_limit_behavior() throws Exception {
        var blockingLatch = new CountDownLatch(1);
        var executionStarted = new AtomicBoolean(false);

        // task that will block until signaled
        failsafeExecutor.registerTask("blockingTask", (String p) -> {
            executionStarted.set(true);
            blockingLatch.await(); // Block until signaled
        });

        // Create a new queue with a small queue size for testing
        var smallQueue = new FailsafeExecutor(
                systemClock,
                dataSource,
                1, // Single worker thread
                3, // Small queue size
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                Duration.ofSeconds(6),
                Duration.ofMillis(150),
                "test-node-small"
        );

        try {
            smallQueue.registerTask("blockingTask", (String p) -> {
                executionStarted.set(true);
                blockingLatch.await(); // Block until signaled
            });

            // Start the queue
            smallQueue.start();

            // Add tasks to fill the queue (1 executing + 3 waiting = 4 total)
            for (int i = 0; i < 4; i++) {
                String id = UUID.randomUUID().toString();
                try (Connection conn = dataSource.getConnection()) {
                    smallQueue.execute(conn, id, "blockingTask", null);
                }
            }

            // Wait for the first task to start executing
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(executionStarted::get);

            // Verify that only the configured number of tasks are picked up
            Awaitility.await()
                    .atMost(2, TimeUnit.SECONDS)
                    .until(() -> smallQueue.activeTaskIds.size() <= 3);

            // The queue should have 1 executing task and 2 waiting (3 total active)
            assertTrue(smallQueue.activeTaskIds.size() <= 3,
                    "Queue should respect the configured size limit");
        } finally {
            // Signal the blocking task to complete
            blockingLatch.countDown();
            // Shutdown the test queue
            smallQueue.shutdown();
        }
    }

    @Test
    void listener_exception_handling() throws Exception {
        FailsafeExecutor.TaskExecutionListener throwingListener = new FailsafeExecutor.TaskExecutionListener() {
            @Override
            public void persisting(String name, String id, String parameter, LocalDateTime plannedExecutionTime) {
                throw new RuntimeException("Listener exception");
            }

            @Override
            public void retrying(String name, String id, String parameter) {
                throw new RuntimeException("Listener exception");
            }

            @Override
            public void succeeded(String name, String id, String parameter) {
                throw new RuntimeException("Listener exception");
            }

            @Override
            public void failed(String name, String id, String parameter, Exception ex) {
                throw new RuntimeException("Listener exception");
            }
        };

        failsafeExecutor.subscribe(throwingListener);

        AtomicInteger errorHandlerCounter = new AtomicInteger(0);
        failsafeExecutor.registerErrorHandler(t -> errorHandlerCounter.incrementAndGet());

        var taskExecuted = new AtomicBoolean(false);
        failsafeExecutor.registerTask("robustTask", (String p) -> {
            taskExecuted.set(true);
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, "robustTask", null);
        }

        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(taskExecuted::get);

        assertTrue(taskExecuted.get(), "Task should execute despite listener exceptions");
        assertTrue(failsafeExecutor.findOne(id).isEmpty(), "Task should be removed after successful execution");
        assertEquals(2, errorHandlerCounter.get());
    }

    @Test
    void record_failure_and_retry() throws Exception {
        var executed = new AtomicBoolean(false);
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        failsafeExecutor.registerTask("robustTask", p -> executed.set(true));
        failsafeExecutor.recordFailure("failedId", "robustTask", "param", new RuntimeException("Naaaaa"));

        Optional<FailsafeExecutor.Task> failsafeTask = failsafeExecutor.findOne("failedId");
        assertTrue(failsafeTask.isPresent());
        FailsafeExecutor.Task task = failsafeTask.get();
        assertNotNull(task.executionFailure().failTime());
        assertEquals("Naaaaa", task.executionFailure().exceptionMessage());

        // Retry should reset fail fields
        try (Connection conn = dataSource.getConnection()) {
            boolean retried = failsafeExecutor.retry(conn, task);
            assertTrue(retried, "Retry should succeed");
        }

        assertFalse(executed.get());

        failsafeExecutor.start();

        Awaitility.await()
                .atMost(500, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.succeededNotified);

        failsafeTask = failsafeExecutor.findOne("failedId");
        assertTrue(failsafeTask.isEmpty());
        assertTrue(executed.get());
    }

    @Test
    void deferred_task() throws Exception {
        failsafeExecutor.start();

        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        failsafeExecutor.registerTask(taskName, (String p) -> executedCount.incrementAndGet());

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, id, taskName, null, systemClock.now().plusMinutes(3));
        }

        Thread.sleep(failsafeExecutor.pollingInterval.toMillis() * 2);

        assertTrue(taskExecutionListener.persistingNotified.get());
        assertFalse(taskExecutionListener.succeededNotified.get());
        assertEquals(0, executedCount.get());

        systemClock.timeTravelBy(Duration.ofMinutes(3));

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 1);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:"));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("success:"));
        assertTrue(failsafeExecutor.isRunning());
        assertTrue(failsafeExecutor.findOne(id).isEmpty());
    }

    @Test
    void waitForTasks_exclude_deferred_tasks() throws Exception {
        failsafeExecutor.start();

        AtomicInteger executedCount = new AtomicInteger(0);
        AtomicInteger deferredExecutedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        failsafeExecutor.registerTask(taskName, p -> executedCount.incrementAndGet());
        String deferredTaskName = "deferredTask";
        failsafeExecutor.registerTask(deferredTaskName, p -> deferredExecutedCount.incrementAndGet());

        String deferredTaskId = UUID.randomUUID().toString();
        String incrementTaskId = UUID.randomUUID().toString();

        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, deferredTaskId, deferredTaskName, null, systemClock.now().plusMinutes(3));
            }

            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, incrementTaskId, taskName, null);
            }

        });

        assertEquals("persist:" + deferredTaskName + "_" + deferredTaskId, taskExecutionListener.listenerOrder.get(0));
        assertEquals("persist:" + taskName + "_" + incrementTaskId, taskExecutionListener.listenerOrder.get(1));
        assertEquals("success:" + taskName + "_" + incrementTaskId, taskExecutionListener.listenerOrder.get(2));
        assertEquals(1, executedCount.get());
        assertEquals(0, deferredExecutedCount.get());

        String deferredTask2Id = UUID.randomUUID().toString();

        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, deferredTask2Id, deferredTaskName, null, systemClock.now().plusMinutes(6));
            }

            systemClock.timeTravelBy(Duration.ofMinutes(3));
        });

        assertEquals("persist:" + deferredTaskName + "_" + deferredTask2Id, taskExecutionListener.listenerOrder.get(3));
        assertEquals("success:" + deferredTaskName + "_" + deferredTaskId, taskExecutionListener.listenerOrder.get(4));
        assertEquals(1, executedCount.get());
        assertEquals(1, deferredExecutedCount.get());
        assertEquals(5, taskExecutionListener.listenerOrder.size());

        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            systemClock.timeTravelBy(Duration.ofMinutes(3));
        });

        assertEquals("success:" + deferredTaskName + "_" + deferredTask2Id, taskExecutionListener.listenerOrder.get(5));
        assertEquals(1, executedCount.get());
        assertEquals(2, deferredExecutedCount.get());
        assertEquals(6, taskExecutionListener.listenerOrder.size());
    }

    @Test
    void waitForTasks_existing_deferred_tasks_do_not_interfere_tasks() throws Exception {
        failsafeExecutor.start();

        AtomicInteger executedCount = new AtomicInteger(0);
        AtomicInteger deferredExecutedCount = new AtomicInteger(0);

        // Register a task
        String taskName = "incrementTask";
        failsafeExecutor.registerTask(taskName, p -> executedCount.incrementAndGet());
        String deferredTaskName = "deferredTask";
        failsafeExecutor.registerTask(deferredTaskName, p -> deferredExecutedCount.incrementAndGet());

        String deferredTaskId = UUID.randomUUID().toString();
        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, deferredTaskId, deferredTaskName, null, systemClock.now().plusMinutes(13));
            }
        });

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        String incrementTaskId = UUID.randomUUID().toString();
        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            try (Connection conn = dataSource.getConnection()) {
                failsafeExecutor.execute(conn, incrementTaskId, taskName, null);
            }
        });

        assertEquals("persist:" + taskName + "_" + incrementTaskId, taskExecutionListener.listenerOrder.get(0));
        assertEquals("success:" + taskName + "_" + incrementTaskId, taskExecutionListener.listenerOrder.get(1));
        assertEquals(1, executedCount.get());
        assertEquals(0, deferredExecutedCount.get());

        FailsafeExecutor.waitForTasks(failsafeExecutor, Duration.ofSeconds(IS_DEBUG ? 60 * 60 : 10), () -> {
            systemClock.timeTravelBy(Duration.ofMinutes(1));
        });

        assertEquals(1, executedCount.get());
        assertEquals(0, deferredExecutedCount.get());
        assertEquals(2, taskExecutionListener.listenerOrder.size());
    }

    /***********************************************************************
     QUEUE TESTS
     ************************************************************************/

    @Test
    void queued_task_is_executed_and_listener_called() throws Exception {
        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        String queueName = "my-queue";
        failsafeExecutor.registerQueue(queueName);
        failsafeExecutor.registerTask(taskName, (String p) -> executedCount.incrementAndGet());

        // Insert task manually (simulating enqueue)
        String id1 = UUID.randomUUID().toString();
        String id2 = UUID.randomUUID().toString();
        String id3 = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, queueName, id1, taskName, "");
            failsafeExecutor.execute(conn, queueName, id2, taskName, "");
            failsafeExecutor.execute(conn, queueName, id3, taskName, "");
        }

        // Start queue and wait for execution
        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofSeconds(2));

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 3);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:%s".formatted(FailsafeExecutor.QUEUE_PREFIX) + queueName));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("persist:incrementTask_" + id1));
        assertTrue(taskExecutionListener.listenerOrder.get(2).startsWith("persist:incrementTask_" + id2));
        assertTrue(taskExecutionListener.listenerOrder.get(3).startsWith("persist:incrementTask_" + id3));
        assertTrue(taskExecutionListener.listenerOrder.get(4).startsWith("success:incrementTask_" + id1));
        assertTrue(taskExecutionListener.listenerOrder.get(5).startsWith("success:incrementTask_" + id2));
        assertTrue(taskExecutionListener.listenerOrder.get(6).startsWith("success:incrementTask_" + id3));
        assertTrue(taskExecutionListener.listenerOrder.get(7).startsWith("success:%s".formatted(FailsafeExecutor.QUEUE_PREFIX) + queueName));
        assertTrue(failsafeExecutor.isRunning());
        assertTrue(failsafeExecutor.findOne(id1).isEmpty());
        assertTrue(failsafeExecutor.findOne(id2).isEmpty());
        assertTrue(failsafeExecutor.findOne(id3).isEmpty());
    }

    @Test
    void queued_task_failure_marks_failed_and_can_retry() throws Exception {
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        String queueName = "my-queue";
        failsafeExecutor.registerQueue(queueName);

        var shouldFail = new AtomicBoolean(true);
        String failingTask = "failingTask";
        failsafeExecutor.registerTask(failingTask, (String p) -> {
            if (shouldFail.get()) {
                throw new RuntimeException("boom!");
            }
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, queueName, id, failingTask, "");
        }

        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofSeconds(2));

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.failedNotified);

        // Ensure fail columns set
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT FAIL_TIME, EXCEPTION_MESSAGE, LOCK_TIME, NODE_ID FROM FAILSAFE_TASK WHERE ID=?")) {
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp("FAIL_TIME"));
            assertNull(rs.getTimestamp("LOCK_TIME"));
            assertNull(rs.getString("NODE_ID"));
            assertEquals("boom!", rs.getString("EXCEPTION_MESSAGE"));
        }

        FailsafeExecutor.Task task = failsafeExecutor.findOne(id).get();
        assertNotNull(task.executionFailure().failTime());
        assertEquals("boom!", task.executionFailure().exceptionMessage());

        failsafeExecutor.shutdown();

        // Retry should reset fail fields
        try (Connection conn = dataSource.getConnection()) {
            boolean retried = failsafeExecutor.retry(conn, task);
            assertTrue(retried, "Retry should succeed");
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT FAIL_TIME, RETRY_COUNT, LOCK_TIME, NODE_ID, PLANNED_EXECUTION_TIME FROM FAILSAFE_TASK WHERE ID=?")) {
            ps.setString(1, id);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getTimestamp("FAIL_TIME"));
            assertNull(rs.getTimestamp("LOCK_TIME"));
            assertNull(rs.getString("NODE_ID"));
            assertEquals(1, rs.getInt("RETRY_COUNT"));
            assertEquals(now, rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime());
        }

        shouldFail.set(false);
        failsafeExecutor.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.succeededNotified);
    }

    @Test
    void execute_task_for_unknown_queue() throws SQLException {
        String taskName = "failingTask";
        failsafeExecutor.registerTask(taskName, p -> {
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.execute(conn, "unknown-queue", id, taskName, ""));
        }
    }

    @Test
    void cancel_failed_queued_task() throws SQLException {
        var taskExecutionListener = new TestTaskExecutionListener();
        failsafeExecutor.subscribe(taskExecutionListener);

        String queueName = "my-queue";
        failsafeExecutor.registerQueue(queueName);

        String failingTask = "failingTask";
        failsafeExecutor.registerTask(failingTask, (String p) -> {
            throw new RuntimeException("boom!");
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            failsafeExecutor.execute(conn, queueName, id, failingTask, "");
        }

        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofSeconds(2));

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.failedNotified);

        FailsafeExecutor.Task task = failsafeExecutor.findOne(id).get();
        failsafeExecutor.cancel(task);

        assertTrue(failsafeExecutor.findOne(id).isEmpty());
    }

    public static class TestTaskExecutionListener implements FailsafeExecutor.TaskExecutionListener {
        public AtomicBoolean persistingNotified = new AtomicBoolean(false);
        public AtomicBoolean retryNotified = new AtomicBoolean(false);
        public AtomicBoolean succeededNotified = new AtomicBoolean(false);
        public AtomicBoolean failedNotified = new AtomicBoolean(false);
        CopyOnWriteArrayList<String> listenerOrder = new CopyOnWriteArrayList<>();

        @Override
        public void persisting(String name, String id, String parameter, LocalDateTime plannedExecutionTime) {
            persistingNotified.set(true);
            listenerOrder.add("persist:" + name + "_" + id);
        }

        @Override
        public void retrying(String name, String id, String parameter) {
            retryNotified.set(true);
            listenerOrder.add("retry:" + name);
        }

        @Override
        public void succeeded(String name, String id, String parameter) {
            succeededNotified.set(true);
            listenerOrder.add("success:" + name + "_" + id);
        }

        @Override
        public void failed(String name, String id, String parameter, Exception ex) {
            failedNotified.set(true);
            listenerOrder.add("fail:" + name);
        }
    }

    public class TestSystemClock implements FailsafeExecutor.SystemClock {

        private Clock clock = Clock.systemDefaultZone();

        @Override
        public LocalDateTime now() {
            return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
        }

        public void timeTravelBy(Duration duration) {
            this.clock = Clock.offset(this.clock, duration);
        }

        public void fixedTime(LocalDateTime now) {
            Instant instant = now.atZone(ZoneId.systemDefault()).toInstant();
            this.clock = Clock.fixed(instant, ZoneId.systemDefault());
        }

        public void resetTime() {
            this.clock = Clock.systemDefaultZone();
        }
    }

}


