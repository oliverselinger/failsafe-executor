package os.failsafe.executor.it;

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
import os.failsafe.executor.FailsafeExecutor;
import os.failsafe.executor.Task;
import os.failsafe.executor.TaskExecutionListener;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.schedule.DailySchedule;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    void throw_an_exception_if_lock_timeout_too_short() {
        assertThrows(IllegalArgumentException.class, () -> new FailsafeExecutor(systemClock, dataSource, 4, 4, Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMinutes(4)));
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
        assertTrue(failsafeExecutor.allTasks().isEmpty());
    }

    @Test
    void execute_a_task() {
        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        failsafeExecutor.start();

        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);
    }

    @Test
    void execute_a_transactional_task() {
        String taskName = "transactionalTask";
        failsafeExecutor.registerTask(taskName, (con, param) -> {
        });
        String taskId = failsafeExecutor.execute(taskName, parameter);
        failsafeExecutor.start();

        assertListenerOnSucceeded(taskName, taskId, parameter);
        assertEquals(0, failsafeExecutor.allTasks().size());
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
        assertEquals(1, failsafeExecutor.failedTasks().size());
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

        assertEquals(0, failsafeExecutor.allTasks().size());
    }

    @Test
    void not_throw_an_exception_if_defered_task_is_already_existing() {
        String taskId = "taskId";
        String actualTaskId = failsafeExecutor.defer(taskId, TASK_NAME, parameter, systemClock.now());

        assertDoesNotThrow(() -> failsafeExecutor.defer(taskId, TASK_NAME, parameter, systemClock.now()));

        assertEquals(1, failsafeExecutor.allTasks().size());
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
    void not_throw_an_exception_if_scheduled_task_already_exists_in_db() throws SQLException {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";

        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World"));

        FailsafeExecutor otherFailsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        assertDoesNotThrow(() -> otherFailsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World")));

        assertEquals(1, failsafeExecutor.allTasks().size());
        assertEquals(scheduleTaskName, taskId);
    }

    @Test
    void throw_an_exception_if_task_is_already_scheduled() {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";

        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World"));
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.schedule(scheduleTaskName, dailySchedule, () -> log.info("Hello World")));

        assertEquals(1, failsafeExecutor.allTasks().size());
        assertEquals(scheduleTaskName, taskId);
    }

    @Test
    void execute_scheduled_parameterized_task_every_day() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";
        final String parameter = "param";
        failsafeExecutor.registerTask(scheduleTaskName, dailySchedule, param -> assertEquals(parameter, param));

        String taskId = failsafeExecutor.execute(scheduleTaskName, parameter);
        assertListenerOnPersisting(scheduleTaskName, taskId, parameter);

        failsafeExecutor.start();

        assertNeverListenerOnSucceededAndOnFailed();

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, parameter);

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, parameter);

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void execute_scheduled_parameterized_task_multiple_times() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";
        final String parameter = "param";
        failsafeExecutor.registerTask(scheduleTaskName, dailySchedule, param -> assertEquals(parameter, param));

        String taskId1 = failsafeExecutor.execute(scheduleTaskName, parameter);
        assertListenerOnPersisting(scheduleTaskName, taskId1, parameter);
        String taskId2 = failsafeExecutor.execute(scheduleTaskName, parameter);
        assertListenerOnPersisting(scheduleTaskName, taskId2, parameter);
        String taskId3 = failsafeExecutor.execute("taskId3", scheduleTaskName, parameter);
        assertListenerOnPersisting(scheduleTaskName, taskId3, parameter);

        assertNotEquals(taskId1, taskId2);
        assertNotEquals(taskId1, taskId3);
        assertNotEquals(taskId2, taskId3);

        assertEquals(taskId3, "taskId3");

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void not_throw_an_exception_if_scheduled_parameterized_task_already_executed() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";
        final String parameter = "param";
        failsafeExecutor.registerTask(scheduleTaskName, dailySchedule, param -> assertEquals(parameter, param));

        String taskId = failsafeExecutor.execute("scheduledTaskId1", scheduleTaskName, parameter);
        String actualTaskId = assertDoesNotThrow(() -> failsafeExecutor.execute("scheduledTaskId1", scheduleTaskName, parameter));

        assertEquals(1, failsafeExecutor.allTasks().size());
        assertEquals(taskId, actualTaskId);
    }

    @Test
    void throw_an_exception_if_scheduled_parameterized_task_is_already_registered() {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";

        failsafeExecutor.registerTask(scheduleTaskName, dailySchedule, param -> assertEquals(parameter, param));
        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.registerTask(scheduleTaskName, dailySchedule, param -> assertEquals(parameter, param)));
    }

    @Test
    void retry_a_failed_task_on_demand() {
        executionShouldFail = true;

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnPersisting(TASK_NAME, taskId, parameter);

        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(TASK_NAME, taskId, parameter, runtimeException);

        List<Task> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failsafeExecutor.task(failedTask.getId()).orElseThrow(() -> new RuntimeException("Should be present"));

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

        List<Task> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failsafeExecutor.cancel(failedTask);

        verifyNoMoreInteractions(taskExecutionListener);
        assertTrue(failsafeExecutor.allTasks().isEmpty());
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
    void execute_all_tasks() {
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
    void retry_a_failed_task_on_demand_and_wait_for_its_execution() {
        executionShouldFail = true;
        failsafeExecutor.start();

        awaitAllTasks(failsafeExecutor, () -> failsafeExecutor.execute(TASK_NAME, parameter), failures -> {
            assertEquals(1, failures.size());
            assertEquals(TASK_NAME, failures.get(0).getName());
            assertEquals(parameter, failures.get(0).getParameter());
        });

        executionShouldFail = false;
        Task failedTask = failsafeExecutor.failedTasks().get(0);

        awaitAllTasks(failsafeExecutor, () -> failsafeExecutor.retry(failedTask), failures -> fail());

        assertTrue(failsafeExecutor.allTasks().isEmpty());
    }

    @Test
    void persist_a_task_as_failed_so_no_execution_is_triggered() {
        failsafeExecutor.recordFailure(TASK_NAME, TASK_NAME, parameter, new RuntimeException("Error"));
        List<Task> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());
        assertEquals("Error", failedTasks.get(0).getExecutionFailure().getExceptionMessage());
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