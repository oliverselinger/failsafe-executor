package os.failsafe.executor;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.schedule.DailySchedule;
import os.failsafe.executor.utils.BlockingRunnable;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.FailsafeExecutorMetricsCollector;
import os.failsafe.executor.utils.TestSystemClock;
import os.failsafe.executor.utils.Transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_LOCK_TIMEOUT;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_QUEUE_SIZE;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_WORKER_THREAD_COUNT;
import static os.failsafe.executor.utils.testing.FailsafeExecutorTestUtility.awaitAllTasks;

class FailsafeExecutorShould {

    private static final Logger log = LoggerFactory.getLogger(FailsafeExecutorShould.class);

    private final TestSystemClock systemClock = new TestSystemClock();

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    DataSource dataSource;
    FailsafeExecutor failsafeExecutor;
    TaskExecutionListener taskExecutionListener;

    boolean executionShouldFail;
    private static final String TASK_NAME = "TestTask";
    private final String parameter = " world!";
    private final RuntimeException runtimeException = new RuntimeException();

    @BeforeEach
    void init() throws SQLException {
        dataSource = DB_EXTENSION.dataSource();
        systemClock.resetTime();

        failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        taskExecutionListener = Mockito.mock(TaskExecutionListener.class);

        failsafeExecutor.registerTask(TASK_NAME, parameter -> {
            if (executionShouldFail) {
                throw runtimeException;
            }

            log.info("Hello {}", parameter);
        });

        failsafeExecutor.subscribe(taskExecutionListener);
    }

    @AfterEach
    void stop() {
        failsafeExecutor.stop();
    }

    @AfterEach
    void createTable(TestInfo info) {
        if (info.getTags().contains("dropTable")) {
            DB_EXTENSION.dropTable();
        }

        if (info.getTags().contains("createTable")) {
            DB_EXTENSION.createTable();
        }
    }

    @Test
    void throw_an_exception_if_queue_size_is_less_than_worker_thread_count() {
        assertThrows(IllegalArgumentException.class, () -> new FailsafeExecutor(systemClock, dataSource, 5, 4, Duration.ofMillis(1), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT));
    }

    @Test
    void not_throw_exception_default_datasource_constructor() {
        assertDoesNotThrow(() -> new FailsafeExecutor(dataSource));
    }

    @Test
    void throw_exception_if_task_is_already_registered() {
        String taskName = "task1";
        failsafeExecutor.registerTask(taskName, parameter -> {
        });
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.registerTask(taskName, parameter -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.registerRemoteTask(taskName));

        String remoteTaskName = "remotetask1";
        failsafeExecutor.registerRemoteTask(remoteTaskName);
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.registerRemoteTask(remoteTaskName));
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.registerTask(remoteTaskName, parameter -> {
        }));
    }

    @Test
    void throw_exception_if_no_task_is_registered() {
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.execute("UNKOWN", parameter));
    }

    @Test
    void notify_listeners_about_task_execution() {
        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnPersisting(TASK_NAME, taskId, parameter);
    }

    @Test
    void notify_listeners_about_remote_task_execution() {
        String taskName = "remoteTask";
        failsafeExecutor.registerRemoteTask(taskName);
        String taskId = failsafeExecutor.execute(taskName, parameter);
        assertListenerOnPersisting(taskName, taskId, parameter);
    }

    @Test
    void not_commit_test_task_of_table_structure_validation() {
        assertTrue(failsafeExecutor.findAll().isEmpty());
    }

    @Test
    void find_a_task() {
        assertEquals(0, failsafeExecutor.findAll().size());
        assertEquals(0, failsafeExecutor.count());

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);

        assertTrue(failsafeExecutor.findOne(taskId).isPresent());
        assertFalse(failsafeExecutor.findOne("abc").isPresent());

        assertEquals(1, failsafeExecutor.findAll().size());
        assertEquals(1, failsafeExecutor.findAll(TASK_NAME, parameter, null, 0, 100).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, null, 0, 100).size());
        assertEquals(0, failsafeExecutor.findAll("abc", null, null, 0, 100).size());
        assertEquals(0, failsafeExecutor.findAll(null, "abc", null, 0, 100).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, true, 0, 100).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, false, 0, 100).size());

        assertEquals(1, failsafeExecutor.count());
        assertEquals(1, failsafeExecutor.count(TASK_NAME, parameter, null));
        assertEquals(1, failsafeExecutor.count(null, null, null));
        assertEquals(0, failsafeExecutor.count("abc", null, null));
        assertEquals(0, failsafeExecutor.count(null, null, true));
        assertEquals(1, failsafeExecutor.count(null, null, false));

        assertEquals(0, failsafeExecutor.findAllFailed().size());
        assertEquals(0, failsafeExecutor.countFailedTasks());
    }

    @Test
    void find_not_failed_task_by_more_filters() {
        String taskIdNotFailed = failsafeExecutor.execute(TASK_NAME, parameter);
        assertTrue(failsafeExecutor.findOne(taskIdNotFailed).isPresent());

        List<Task> all = failsafeExecutor.findAll();
        assertEquals(1, all.size());

        LocalDateTime from = LocalDateTime.now().minusMinutes(1);
        LocalDateTime dateTo = LocalDateTime.now().plusMinutes(1);
        assertEquals(1, failsafeExecutor.findAll(null, null, null, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(TASK_NAME, null, null, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, from, dateTo, from, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, from, dateTo, null, dateTo, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, true, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, from, dateTo, from, dateTo, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, "ERROR MESSAGE", null, null, null, null, 0, 10).size());

        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, LocalDateTime.now().plusMinutes(1), null, null, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, null, null, LocalDateTime.now().plusMinutes(1), null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, null, null, LocalDateTime.now().plusMinutes(1), null, 0, 10).size());
    }

    @Test
    void find_failed_task_by_more_filters() throws Exception {
        executionShouldFail = true;
        failsafeExecutor.start();
        awaitAllTasks(failsafeExecutor, () -> failsafeExecutor.execute(TASK_NAME, parameter), failures -> {
            assertEquals(1, failures.size());
            assertEquals(TASK_NAME, failures.get(0).getName());
            assertEquals(parameter, failures.get(0).getParameter());
        });

        List<Task> all = failsafeExecutor.findAll();
        assertEquals(1, all.size());

        LocalDateTime from = LocalDateTime.now().minusMinutes(1);
        LocalDateTime dateTo = LocalDateTime.now().plusMinutes(1);
        assertEquals(1, failsafeExecutor.findAll(null, null, null, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(TASK_NAME, null, null, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, null, null, from, dateTo, from, null, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, null, null, from, dateTo, null, dateTo, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, true, null, from, dateTo, null, null, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, null, null, from, dateTo, from, dateTo, 0, 10).size());
        assertEquals(1, failsafeExecutor.findAll(null, null, null, "", null, null, null, null, 0, 10).size());

        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, dateTo, null, null, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, null, null, dateTo, null, 0, 10).size());
        assertEquals(0, failsafeExecutor.findAll(null, null, null, null, null, null, dateTo, null, 0, 10).size());
    }

    @Test
    void execute_a_task() {
        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);

        assertEquals(1, failsafeExecutor.count());

        failsafeExecutor.start();

        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);
        assertEquals(0, failsafeExecutor.count());
    }

    @Test
    void throw_exception_if_nodeId_too_long(){
        String id = UUID.randomUUID().toString() + UUID.randomUUID().toString();
        String nodeId = id.substring(0, FailsafeExecutor.NODE_ID_MAX_LENGTH + 1);

        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.start(nodeId));
    }

    @Test
    void not_throw_exception_if_nodeID_null(){
        assertDoesNotThrow(() -> failsafeExecutor.start());
    }

    @Test
    void execute_a_transactional_task() {
        String taskName = "transactionalTask";
        failsafeExecutor.registerTask(taskName, (con, param) -> {
        });
        String taskId = failsafeExecutor.execute(taskName, parameter);

        assertEquals(1, failsafeExecutor.count());

        failsafeExecutor.start();

        assertListenerOnSucceeded(taskName, taskId, parameter);
        assertEquals(0, failsafeExecutor.count());
    }

    @Test
    void execute_and_rollback_a_transactional_task() {
        String taskName = "transactionalTask";
        failsafeExecutor.registerTask(taskName, (con, param) -> {
            throw new RuntimeException("Rollback");
        });
        String taskId = failsafeExecutor.execute(taskName, parameter);
        failsafeExecutor.start();

        assertListenerOnFailed(taskName, taskId, parameter);
        assertEquals(1, failsafeExecutor.findAllFailed().size());
        assertEquals(1, failsafeExecutor.countFailedTasks());
    }

    @Test
    void defer_task_execution_to_the_planned_time() {
        failsafeExecutor.start();

        LocalDateTime plannedExecutionTime = LocalDateTime.of(2020, 5, 1, 10, 0);
        systemClock.fixedTime(plannedExecutionTime.minusDays(1));

        String taskId = failsafeExecutor.defer(TASK_NAME, parameter, plannedExecutionTime);
        assertListenerOnPersisting(TASK_NAME, taskId, parameter);

        verify(taskExecutionListener, times(0)).succeeded(TASK_NAME, taskId, parameter);

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);

        assertEquals(0, failsafeExecutor.findAll().size());
    }


    @Test
    void not_throw_an_exception_if_defered_task_is_already_existing() {
        String taskId = "taskId";
        String actualTaskId = failsafeExecutor.defer(taskId, TASK_NAME, parameter, systemClock.now());

        assertDoesNotThrow(() -> failsafeExecutor.defer(taskId, TASK_NAME, parameter, systemClock.now()));

        assertEquals(1, failsafeExecutor.findAll().size());
        assertEquals(taskId, actualTaskId);
    }

    @Test
    void schedule_execution_a_daily_task_every_day() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";

        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World"));
        assertListenerOnPersisting(scheduleTaskName, taskId, null);

        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void not_schedule_failed_task() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";
        AtomicBoolean shouldThrow = new AtomicBoolean(true);
        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> {
            if (shouldThrow.get())
                throw new Exception("Not working");
        });
        assertListenerOnPersisting(scheduleTaskName, taskId, null);

        failsafeExecutor.start();

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnFailed(scheduleTaskName, taskId, null);

        Task task = failsafeExecutor.findOne(taskId).get();
        assertEquals(beforePlannedExecutionTime.plusSeconds(1), task.getPlannedExecutionTime());

        systemClock.timeTravelBy(Duration.ofDays(1));
        verifyNoMoreInteractions(taskExecutionListener);

        shouldThrow.set(false);
        failsafeExecutor.retry(task);
        assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        task = failsafeExecutor.findOne(taskId).get();
        assertEquals(beforePlannedExecutionTime.plusSeconds(1).plusDays(2), task.getPlannedExecutionTime());
    }

    @Test
    void not_throw_an_exception_if_scheduled_task_already_exists_in_db() throws SQLException {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";

        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World"));

        FailsafeExecutor otherFailsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        assertDoesNotThrow(() -> otherFailsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World")));

        assertEquals(1, failsafeExecutor.findAll().size());
        assertEquals(scheduleTaskName, taskId);
    }

    @Test
    void throw_an_exception_if_task_is_already_scheduled() {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";

        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World"));
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World")));

        assertEquals(1, failsafeExecutor.findAll().size());
        assertEquals(scheduleTaskName, taskId);
    }

    @Test
    void retry_a_failed_task_on_demand() {
        executionShouldFail = true;

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnPersisting(TASK_NAME, taskId, parameter);

        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(TASK_NAME, taskId, parameter, runtimeException);

        List<Task> failedTasks = failsafeExecutor.findAllFailed();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failsafeExecutor.findOne(failedTask.getId()).orElseThrow(() -> new RuntimeException("Should be present"));

        executionShouldFail = false;

        failsafeExecutor.retry(failedTask);

        assertListenerOnRetrying(TASK_NAME, taskId, parameter);
        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void cancel_a_failed_task_on_demand() {
        executionShouldFail = true;

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnPersisting(TASK_NAME, taskId, parameter);

        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(TASK_NAME, taskId, parameter, runtimeException);

        List<Task> failedTasks = failsafeExecutor.findAllFailed();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failsafeExecutor.cancel(failedTask);

        verifyNoMoreInteractions(taskExecutionListener);
        assertTrue(failsafeExecutor.findAll().isEmpty());
    }

    @Test
    @Tag("createTable")
    void fail_on_instantiation_when_table_does_not_exist() {
        DB_EXTENSION.dropTable();
        assertThrows(RuntimeException.class, () -> new FailsafeExecutor(dataSource));
    }

    @Test
    @Tag("dropTable")
    @Tag("createTable")
    void fail_on_instantiation_when_column_is_missing() {
        DB_EXTENSION.deleteColumn("RETRY_COUNT");
        assertThrows(RuntimeException.class, () -> new FailsafeExecutor(dataSource));
    }

    @Test
    void report_failures() throws SQLException {
        DataSource failingDataSource = Mockito.spy(dataSource);
        FailsafeExecutor failsafeExecutor = new FailsafeExecutor(systemClock, failingDataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        failsafeExecutor.registerTask(TASK_NAME, parameter -> {
        });

        RuntimeException connectionException = new RuntimeException("Error");
        doThrow(connectionException).when(failingDataSource).getConnection();

        failsafeExecutor.start();

        Awaitility
                .await()
                .pollDelay(Durations.ONE_MILLISECOND)
                .timeout(Duration.ofSeconds(3))
                .until(failsafeExecutor::isLastRunFailed);

        failsafeExecutor.stop(3, TimeUnit.SECONDS);

        assertTrue(failsafeExecutor.isLastRunFailed());
        assertEquals(connectionException, failsafeExecutor.lastRunException());
    }

    @Test
    void execute_all_tasks() throws Exception {
        failsafeExecutor.start();

        int taskCount = 5;
        List<String> parameters = IntStream.range(0, taskCount).mapToObj(String::valueOf).collect(Collectors.toList());

        awaitAllTasks(failsafeExecutor, () -> {
            try (Connection connection = dataSource.getConnection();
                 Transaction transaction = new Transaction(connection)) {

                parameters.forEach(param -> failsafeExecutor.execute(connection, TASK_NAME, param));

                transaction.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, failedTasks -> fail(failedTasks.toString()));

        ArgumentCaptor<String> parameterCaptor = ArgumentCaptor.forClass(String.class);
        verify(taskExecutionListener, times(taskCount)).persisting(eq(TASK_NAME), any(), any());
        verify(taskExecutionListener, times(taskCount)).succeeded(eq(TASK_NAME), any(), parameterCaptor.capture());

        assertTrue(parameterCaptor.getAllValues().containsAll(parameters));
    }

    @Test
    void retry_a_failed_task_on_demand_and_wait_for_its_execution() throws Exception {
        executionShouldFail = true;
        failsafeExecutor.start();

        awaitAllTasks(failsafeExecutor, () -> failsafeExecutor.execute(TASK_NAME, parameter), failures -> {
            assertEquals(1, failures.size());
            assertEquals(TASK_NAME, failures.get(0).getName());
            assertEquals(parameter, failures.get(0).getParameter());
        });

        executionShouldFail = false;
        Task failedTask = failsafeExecutor.findAllFailed().get(0);

        awaitAllTasks(failsafeExecutor, () -> failsafeExecutor.retry(failedTask), failures -> fail());

        assertTrue(failsafeExecutor.findAll().isEmpty());
    }

    @Test
    void persist_a_task_as_failed_so_no_execution_is_triggered() {
        failsafeExecutor.recordFailure(TASK_NAME, TASK_NAME, parameter, new RuntimeException("Error"));
        List<Task> failedTasks = failsafeExecutor.findAllFailed();
        assertEquals(1, failedTasks.size());
        assertEquals("Error", failedTasks.get(0).getExecutionFailure().getExceptionMessage());
    }

    @Test
    void record_failure_with_a_very_long_stack_trace() {
        StackTraceElement[] stackTrace = new StackTraceElement[5_000];
        for (int i = 0; i < stackTrace.length; i++) {
            stackTrace[i] = new StackTraceElement("abc.abc.abc.StackTraceEx", "show", "StackTraceEx.java", 9);
        }

        RuntimeException exception = new RuntimeException("Error");
        exception.setStackTrace(stackTrace);

        String expected = ExceptionUtils.stackTraceAsString(exception);

        failsafeExecutor.recordFailure(TASK_NAME, TASK_NAME, parameter, exception);
        List<Task> failedTasks = failsafeExecutor.findAllFailed();
        assertEquals(1, failedTasks.size());
        assertEquals("Error", failedTasks.get(0).getExecutionFailure().getExceptionMessage());
        assertEquals(expected, failedTasks.get(0).getExecutionFailure().getStackTrace());
    }

    @Test
    void update_lock_until_task_has_finished() throws SQLException {
        FailsafeExecutor failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), Duration.ofMillis(40));
        failsafeExecutor.start();

        BlockingRunnable firstBlockingRunnable = new BlockingRunnable();
        failsafeExecutor.registerTask("LONG_RUNNING_TASK", parameter -> {
            firstBlockingRunnable.run();
        });
        String taskId = failsafeExecutor.execute("LONG_RUNNING_TASK", "ignore");

        firstBlockingRunnable.waitForSetup();

        LocalDateTime lockTime = failsafeExecutor.findOne(taskId).get().getLockTime();

        assertNotNull(lockTime);

        Awaitility
                .await()
                .pollDelay(Durations.ONE_MILLISECOND)
                .timeout(Duration.ofSeconds(3))
                .until(() -> failsafeExecutor.findOne(taskId).get().getLockTime().isAfter(lockTime));

        firstBlockingRunnable.release();
        failsafeExecutor.stop(3, TimeUnit.SECONDS);
    }

    @Test
    void collect_metrics() throws SQLException {
        FailsafeExecutor failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, 10, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), Duration.ofSeconds(10));
        failsafeExecutor.start();
        FailsafeExecutorMetricsCollector metricsCollector = new FailsafeExecutorMetricsCollector();
        failsafeExecutor.subscribe(metricsCollector);

        int parties = 11;
        BlockingRunnable firstBlockingRunnable = new BlockingRunnable(parties);
        failsafeExecutor.registerTask("LONG_RUNNING_TASK", parameter -> {
            firstBlockingRunnable.run();
        });

        for (int i = 0; i < parties - 1; i++) {
            failsafeExecutor.execute("LONG_RUNNING_TASK", "ignore");
        }

        firstBlockingRunnable.waitForSetup();

        firstBlockingRunnable.release();
        failsafeExecutor.stop(3, TimeUnit.SECONDS);

        FailsafeExecutorMetricsCollector.Result collect = metricsCollector.collect();
        assertEquals(10, collect.persistedSum);
        assertEquals(10, collect.finishedSum);
        assertEquals(0, collect.failedSum);
        assertTrue(collect.persistingRateInTargetTimeUnit > 0);
        assertTrue(collect.finishingRateInTargetTimeUnit > 0);
        assertEquals(0, collect.failureRateInTargetTimeUnit);
    }

    private void assertListenerOnPersisting(String name, String taskId, String parameter) {
        verify(taskExecutionListener).persisting(name, taskId, parameter);
    }

    private void assertListenerOnRetrying(String name, String taskId, String parameter) {
        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).retrying(name, taskId, parameter);
    }

    private void assertListenerOnSucceeded(String name, String taskId, String parameter) {
        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).succeeded(name, taskId, parameter);
    }

    private void assertListenerOnFailed(String name, String taskId, String parameter) {
        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(eq(name), eq(taskId), eq(parameter), any());
    }

    private void assertNeverListenerOnSucceededAndOnFailed() {
        verify(taskExecutionListener, never()).succeeded(any(), any(), any());
        verify(taskExecutionListener, never()).failed(any(), any(), any(), any());
    }
}