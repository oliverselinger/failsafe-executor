package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.schedule.OneTimeSchedule;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionShould {

    private final TestSystemClock systemClock = new TestSystemClock();
    private TaskExecutionListener listener;
    private Task task;
    private Consumer<String> runnable;
    private Database database;
    private Connection connection;
    private OneTimeSchedule oneTimeSchedule;
    private TaskRepository taskRepository;
    private final String taskId = "123";
    private final String parameter = "Hello world!";
    private final String taskName = "TestTask";

    @BeforeEach
    void init() throws SQLException {
        runnable = Mockito.mock(Consumer.class);

        oneTimeSchedule = Mockito.mock(OneTimeSchedule.class);
        when(oneTimeSchedule.nextExecutionTime(any())).thenReturn(Optional.empty());

        task = Mockito.mock(Task.class);
        when(task.getId()).thenReturn(taskId);
        when(task.getParameter()).thenReturn(parameter);
        when(task.getName()).thenReturn(taskName);

        listener = Mockito.mock(TaskExecutionListener.class);

        taskRepository = Mockito.mock(TaskRepository.class);

        database = Mockito.mock(Database.class);
        connection = Mockito.mock(Connection.class);

        doAnswer(ans -> {
            Database.ConnectionConsumer connectionConsumer = (Database.ConnectionConsumer) ans.getArguments()[0];
            connectionConsumer.accept(connection);
            return null;
        }).when(database).transaction(any());
    }

    @Test
    void execute_regular_task_with_parameter() {
        Execution execution = createExecutionForRegularTask();
        execution.perform();

        verify(runnable).accept(parameter);
        // notify_listeners_after_successful_execution
        verify(listener).succeeded(taskName, taskId, parameter);
        // delete_task_after_successful_execution
        verify(taskRepository).delete(task);
    }

    @Test
    void execute_transactional_task_with_parameter() {
        Execution execution = createExecutionForTransactionalTask();
        execution.perform();

        verify(runnable).accept(parameter);
        // notify_listeners_after_successful_execution
        verify(listener).succeeded(taskName, taskId, parameter);
        // delete_task_after_successful_execution
        verify(taskRepository).delete(connection, task);
    }

    @Test
    void unlock_task_and_set_next_planned_execution_time_if_one_is_available() {
        Execution scheduledExecution = createExecutionForScheduledTask();

        LocalDateTime nextPlannedExecutionTime = systemClock.now().plusDays(1);
        when(oneTimeSchedule.nextExecutionTime(any())).thenReturn(Optional.of(nextPlannedExecutionTime));

        scheduledExecution.perform();

        verify(taskRepository).unlock(task, nextPlannedExecutionTime);
        verify(taskRepository, never()).delete(any());
    }

    @Test
    void save_failure_on_exception() {
        Execution execution = createExecutionForRegularTask();

        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(runnable).accept(any());

        execution.perform();

        verify(taskRepository).saveFailure(eq(task), any(ExecutionFailure.class));
        // notify_listeners_after_failed_execution
        verify(listener).failed(taskName, taskId, parameter, exception);
    }

    private Execution createExecutionForRegularTask() {
        FailsafeExecutor.TaskRegistration taskRegistration = new FailsafeExecutor.TaskRegistration(taskName, param -> runnable.accept(param));
        return new Execution(database, task, taskRegistration, Collections.singletonList(listener), systemClock, taskRepository);
    }

    private Execution createExecutionForTransactionalTask() {
        FailsafeExecutor.TaskRegistration taskRegistration = new FailsafeExecutor.TaskRegistration(taskName, (con, param) -> runnable.accept(param));
        return new Execution(database, task, taskRegistration, Collections.singletonList(listener), systemClock, taskRepository);
    }

    private Execution createExecutionForScheduledTask() {
        FailsafeExecutor.TaskRegistration taskRegistration = new FailsafeExecutor.TaskRegistration(taskName, oneTimeSchedule, param -> runnable.accept(param));
        return new Execution(database, task, taskRegistration, Collections.singletonList(listener), systemClock, taskRepository);
    }
}
