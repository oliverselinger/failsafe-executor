package os.failsafe.executor.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
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

    @BeforeEach
    void init() {
        dataSource = DB_EXTENSION.dataSource();
        systemClock.resetTime();

        failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        taskExecutionListener = Mockito.mock(TaskExecutionListener.class);

        failsafeExecutor.registerTask(TASK_NAME, (parameter) -> {
            if (executionShouldFail) {
                throw new RuntimeException();
            }

            log.info("Hello {}", parameter);
        });

        failsafeExecutor.subscribe(taskExecutionListener);
    }

    @AfterEach
    void stop() {
        failsafeExecutor.stop();
    }

    @Test
    void throw_an_exception_if_queue_size_is_less_than_worker_thread_count() {
        assertThrows(IllegalArgumentException.class, () -> new FailsafeExecutor(systemClock, dataSource, 5, 4, Duration.ofMillis(1), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT));
    }

    @Test
    void throw_an_exception_if_lock_timeout_too_short() {
        assertThrows(IllegalArgumentException.class, () -> new FailsafeExecutor(systemClock, dataSource, 5, 4, Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofMinutes(4)));
    }

    @Test
    void notify_listeners_about_task_registration() {
        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnRegistration(TASK_NAME, taskId, parameter);
    }

    @Test
    void execute_a_task() {
        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        failsafeExecutor.start();

        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);
    }

    @Test
    void execute_a_daily_scheduled_task_every_day() {
        LocalTime dailyTime = LocalTime.of(1, 0);

        LocalDateTime beforePlannedExecutionTime = LocalDateTime.of(LocalDate.of(2020, 5, 1), dailyTime.minusSeconds(1));
        systemClock.fixedTime(beforePlannedExecutionTime);

        DailySchedule dailySchedule = new DailySchedule(dailyTime);

        final String scheduleTaskName = "ScheduledTestTask";

        failsafeExecutor.registerTask(scheduleTaskName, (parameter) -> log.info("Hello World"));


        String taskId = failsafeExecutor.schedule(scheduleTaskName, dailySchedule);
        assertListenerOnRegistration(scheduleTaskName, taskId, null);

        failsafeExecutor.start();

        //systemClock.timeTravelBy(Duration.ofSeconds(1));
        //assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        systemClock.timeTravelBy(Duration.ofDays(1));
        assertListenerOnSucceeded(scheduleTaskName, taskId, null);

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void not_throw_an_exception_if_scheduled_task_already_exists_in_db() {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";
        failsafeExecutor.registerTask(scheduleTaskName, (parameter) -> log.info("Hello World"));

        failsafeExecutor.schedule(scheduleTaskName, dailySchedule);

        FailsafeExecutor otherFailsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        assertDoesNotThrow(() -> otherFailsafeExecutor.schedule(scheduleTaskName, dailySchedule));
    }

    @Test
    void not_throw_an_exception_if_task_is_already_scheduled() {
        DailySchedule dailySchedule = new DailySchedule(LocalTime.now());

        final String scheduleTaskName = "ScheduledTestTask";
        failsafeExecutor.registerTask(scheduleTaskName, (parameter) -> log.info("Hello World"));

        failsafeExecutor.schedule(scheduleTaskName, dailySchedule);
        assertDoesNotThrow(() -> failsafeExecutor.schedule(scheduleTaskName, dailySchedule));
    }

    @Test
    void retry_a_failed_task_on_demand() {
        executionShouldFail = true;

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnRegistration(TASK_NAME, taskId, parameter);

        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(TASK_NAME, taskId, parameter);

        List<Task> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failsafeExecutor.task(failedTask.getId()).orElseThrow(() -> new RuntimeException("Should be present"));

        executionShouldFail = false;

        failedTask.retry();

        assertListenerOnSucceeded(TASK_NAME, taskId, parameter);

        verifyNoMoreInteractions(taskExecutionListener);
    }

    @Test
    void cancel_a_failed_task_on_demand() {
        executionShouldFail = true;

        String taskId = failsafeExecutor.execute(TASK_NAME, parameter);
        assertListenerOnRegistration(TASK_NAME, taskId, parameter);

        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(TASK_NAME, taskId, parameter);

        List<Task> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);

        failedTask.cancel();

        verifyNoMoreInteractions(taskExecutionListener);
        assertTrue(failsafeExecutor.allTasks().isEmpty());
    }

    @Test
    void report_failures() throws SQLException {
        RuntimeException connectionException = new RuntimeException("Error");

        Connection connection = createFailingJdbcConnection(connectionException);

        DataSource failingDataSource = Mockito.mock(DataSource.class);
        when(failingDataSource.getConnection()).thenReturn(connection);

        FailsafeExecutor failsafeExecutor = new FailsafeExecutor(systemClock, failingDataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofSeconds(15), DEFAULT_LOCK_TIMEOUT);

        failsafeExecutor.registerTask(TASK_NAME, (parameter) -> {
            if (executionShouldFail) {
                throw new RuntimeException();
            }

            log.info("Hello {}", parameter);
        });

        failsafeExecutor.start();

        verify(connection, timeout(TimeUnit.SECONDS.toMillis(50))).prepareStatement(any());
        failsafeExecutor.stop();

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
        });

        ArgumentCaptor<String> parameterCaptor = ArgumentCaptor.forClass(String.class);
        verify(taskExecutionListener, times(taskCount)).registered(eq(TASK_NAME), any(), any());
        verify(taskExecutionListener, times(taskCount)).succeeded(eq(TASK_NAME), any(), parameterCaptor.capture());

        assertTrue(parameterCaptor.getAllValues().containsAll(parameters));
    }

    private Connection createFailingJdbcConnection(RuntimeException e) throws SQLException {
        Connection connection = Mockito.mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getDatabaseProductName()).thenReturn("H2");
        when(connection.prepareStatement(any())).thenThrow(e);
        return connection;
    }

    private void assertListenerOnSucceeded(String name, String taskId, String parameter) {
        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).succeeded(name, taskId, parameter);
    }

    private void assertListenerOnRegistration(String name, String taskId, String parameter) {
        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).registered(name, taskId, parameter);
    }
}