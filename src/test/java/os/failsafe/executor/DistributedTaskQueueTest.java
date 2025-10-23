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
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DistributedTaskQueueTest {

    @RegisterExtension
    static final TestcontainersDbExtension DB_EXTENSION = new TestcontainersDbExtension();

    private static DataSource dataSource;
    private DistributedTaskQueue queue;
    private LocalDateTime now;
    private TestSystemClock systemClock;

    @BeforeAll
    static void beforeAll() {
        dataSource = DB_EXTENSION.dataSource();
    }

    @BeforeEach
    void setUp() throws Exception {
        now = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        systemClock = new TestSystemClock();
        systemClock.fixedTime(now);

        queue = new DistributedTaskQueue(
                systemClock,
                dataSource,
                2, // worker threads
                10, // queue size
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                Duration.ofSeconds(6),
                "test-node"
        );

        queue.registerErrorHandler(Throwable::printStackTrace);
    }

    @AfterEach
    void tearDown() {
        queue.shutdown();
    }

    @Test
    @Order(0)
    void task_is_executed_and_listener_called() throws Exception {
        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        queue.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        queue.registerTask(taskName, (String p) -> executedCount.incrementAndGet());

        // Insert task manually (simulating enqueue)
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, id, taskName, null);
        }

        // Start queue and wait for execution
        queue.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 1);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:"));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("success:"));
        assertTrue(queue.isRunning());
        assertNull(queue.findOne(id));
    }

    @Test
    @Order(1)
    void transactional_task_is_executed_and_listener_called() throws Exception {
        AtomicInteger executedCount = new AtomicInteger(0);

        var taskExecutionListener = new TestTaskExecutionListener();
        queue.subscribe(taskExecutionListener);

        // Register a task
        String taskName = "incrementTask";
        queue.registerTask(taskName, (conn, param) -> {
            executedCount.incrementAndGet();
            assertFalse(conn.getAutoCommit());
        });

        // Insert task manually (simulating enqueue)
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, id, taskName, null);
        }

        // Start queue and wait for execution
        queue.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .until(() -> executedCount.get() == 1);

        assertTrue(taskExecutionListener.succeededNotified.get(), "Listener should be notified of success");
        assertTrue(taskExecutionListener.listenerOrder.get(0).startsWith("persist:"));
        assertTrue(taskExecutionListener.listenerOrder.get(1).startsWith("success:"));
        assertTrue(queue.isRunning());
        assertNull(queue.findOne(id));
    }

    @Test
    @Order(2)
    void task_failure_marks_failed_and_can_retry() throws Exception {
        var taskExecutionListener = new TestTaskExecutionListener();
        queue.subscribe(taskExecutionListener);

        var shouldFail = new AtomicBoolean(true);
        String failingTask = "failingTask";
        queue.registerTask(failingTask, (String p) -> {
            if(shouldFail.get()) {
                throw new RuntimeException("boom!");
            }
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, id, failingTask, null);
        }

        queue.start();

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

        DistributedTaskQueue.Task task = queue.findOne(id);
        assertNotNull(task.executionFailure().failTime());
        assertEquals("boom!", task.executionFailure().exceptionMessage());

        queue.shutdown();

        // Retry should reset fail fields
        boolean retried = queue.retry(id);
        assertTrue(retried, "Retry should succeed");

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
        queue.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.succeededNotified);
    }

    @Test
    @Order(3)
    void schedule_creates_task_and_reschedules_on_completion() throws Exception {
        AtomicInteger runs = new AtomicInteger(0);

        DistributedTaskQueue.Schedule everySecond = base ->
                Optional.of(base.plusSeconds(1)); // every second

        queue.schedule("scheduledTask", everySecond, runs::incrementAndGet);
        queue.start();

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
    @Order(4)
    void start_and_shutdown_change_running_state() {
        assertFalse(queue.isRunning());
        queue.start();
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(queue::isRunning);
        queue.shutdown();
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> !queue.isRunning());
        assertFalse(queue.isRunning());
    }

    @Test
    @Order(5)
    void heartbeat_extends_lock_time_for_active_tasks() throws Exception {
        AtomicBoolean taskStarted = new AtomicBoolean(false);
        AtomicBoolean taskFinished = new AtomicBoolean(false);

        String taskName = "longRunningTask";
        String taskId = UUID.randomUUID().toString();

        // Register a task that blocks until we allow it to finish
        queue.registerTask(taskName, (String p) -> {
            taskStarted.set(true);
            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilTrue(taskFinished); // block until test signals
        });

        // Insert the task into DB
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, taskId, taskName, null);
        }

        // Start the queue
        queue.start();

        // Wait until the task starts running
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .untilTrue(taskStarted);

        assertTrue(queue.activeTaskIds.contains(taskId));

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
        systemClock.timeTravelBy(queue.heartbeatInterval);

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
                .until(() -> !queue.activeTaskIds.contains(taskId));
    }

    @Test
    @Order(6)
    void cancel_failed_task() throws SQLException, IOException {
        var taskExecutionListener = new TestTaskExecutionListener();
        queue.subscribe(taskExecutionListener);

        String failingTask = "failingTask";
        queue.registerTask(failingTask, (String p) -> {
            throw new RuntimeException("boom!");
        });

        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, id, failingTask, null);
        }

        queue.start();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(50))
                .untilTrue(taskExecutionListener.failedNotified);

        queue.cancel(id);

        assertNull(queue.findOne(id));
    }

    @Test
    @Order(7)
    void remote_task_should_be_registered_but_never_executed() throws Exception {
        var taskExecutionListener = new TestTaskExecutionListener();
        queue.subscribe(taskExecutionListener);

        AtomicBoolean executed = new AtomicBoolean(false);
        queue.registerRemoteTask("remoteTask");

        // Add both tasks into the DB
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, "2", "remoteTask", null, LocalDateTime.now());
        }

        // Start the queue
        queue.start();

        Thread.sleep(300);

        var remoteTask = queue.findOne("2");

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
    @Order(8)
    void concurrent_task_execution_with_multiple_worker_threads() throws Exception {
        // Create a latch to track concurrent execution
        var startLatch = new CountDownLatch(1);
        var executionLatch = new CountDownLatch(2);
        var concurrentExecutionTracker = new AtomicInteger(0);
        var maxConcurrentExecution = new AtomicInteger(0);
        
        // Register a task that will block until signaled
        queue.registerTask("concurrentTask", (String p) -> {
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
                queue.execute(conn, id, "concurrentTask", null);
            }
        }
        
        queue.start();
        
        // Allow tasks to start executing
        startLatch.countDown();
        
        // Wait for both tasks to complete
        executionLatch.await(10, TimeUnit.SECONDS);
        
        // Verify that tasks executed concurrently
        assertEquals(2, maxConcurrentExecution.get(), 
                "Both tasks should have executed concurrently");
    }

    @Test
    @Order(10)
    void queue_size_limit_behavior() throws Exception {
        var blockingLatch = new CountDownLatch(1);
        var executionStarted = new AtomicBoolean(false);
        
        // task that will block until signaled
        queue.registerTask("blockingTask", (String p) -> {
            executionStarted.set(true);
            blockingLatch.await(); // Block until signaled
        });
        
        // Create a new queue with a small queue size for testing
        var smallQueue = new DistributedTaskQueue(
                systemClock,
                dataSource,
                1, // Single worker thread
                3, // Small queue size
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                Duration.ofSeconds(6),
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
    @Order(11)
    void listener_exception_handling() throws Exception {
        DistributedTaskQueue.TaskExecutionListener throwingListener = new DistributedTaskQueue.TaskExecutionListener() {
            @Override
            public void persisting(String name, String id, String parameter) {
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

        queue.subscribe(throwingListener);

        AtomicInteger errorHandlerCounter = new AtomicInteger(0);
        queue.registerErrorHandler(t -> errorHandlerCounter.incrementAndGet());

        var taskExecuted = new AtomicBoolean(false);
        queue.registerTask("robustTask", (String p) -> {
            taskExecuted.set(true);
        });
        
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            queue.execute(conn, id, "robustTask", null);
        }
        
        queue.start();
        
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(taskExecuted::get);
        
        assertTrue(taskExecuted.get(), "Task should execute despite listener exceptions");
        assertNull(queue.findOne(id), "Task should be removed after successful execution");
        assertEquals(2, errorHandlerCounter.get());
    }

    public static class TestTaskExecutionListener implements DistributedTaskQueue.TaskExecutionListener {
        public AtomicBoolean persistingNotified = new AtomicBoolean(false);
        public AtomicBoolean succeededNotified = new AtomicBoolean(false);
        public AtomicBoolean failedNotified = new AtomicBoolean(false);
        CopyOnWriteArrayList<String> listenerOrder = new CopyOnWriteArrayList<>();

        @Override
        public void persisting(String name, String id, String parameter) {
            persistingNotified.set(true);
            listenerOrder.add("persist:" + name);
        }

        @Override
        public void retrying(String name, String id, String parameter) {
            listenerOrder.add("retry:" + name);
        }

        @Override
        public void succeeded(String name, String id, String parameter) {
            succeededNotified.set(true);
            listenerOrder.add("success:" + name);
        }

        @Override
        public void failed(String name, String id, String parameter, Exception ex) {
            failedNotified.set(true);
            listenerOrder.add("fail:" + name);
        }
    }

    public class TestSystemClock implements DistributedTaskQueue.SystemClock {

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


