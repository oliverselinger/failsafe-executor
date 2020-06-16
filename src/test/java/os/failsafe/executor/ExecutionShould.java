package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.schedule.OneTimeSchedule;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionShould {

    private final TestSystemClock systemClock = new TestSystemClock();
    private Execution execution;
    private TaskExecutionListener listener;
    private Task task;
    private Consumer<String> runnable;
    private OneTimeSchedule oneTimeSchedule;
    private TaskRepository taskRepository;
    private final String taskId = "123";
    private final String parameter = "Hello world!";
    private final String taskName = "TestTask";

    @BeforeEach
    void init() {
        runnable = Mockito.mock(Consumer.class);

        oneTimeSchedule = Mockito.mock(OneTimeSchedule.class);
        when(oneTimeSchedule.nextExecutionTime(any())).thenReturn(Optional.empty());

        task = Mockito.mock(Task.class);
        when(task.getId()).thenReturn(taskId);
        when(task.getParameter()).thenReturn(parameter);
        when(task.getName()).thenReturn(taskName);

        listener = Mockito.mock(TaskExecutionListener.class);

        taskRepository = Mockito.mock(TaskRepository.class);

        execution = new Execution(task, () -> runnable.accept(parameter), Collections.singletonList(listener), oneTimeSchedule, systemClock, taskRepository);
    }

    @Test
    void execute_task_with_parameter() {
        execution.perform();

        verify(runnable).accept(parameter);
    }

    @Test
    void notify_listeners_after_successful_execution() {
        execution.perform();

        verify(listener).succeeded(taskName, taskId, parameter);
    }

    @Test
    void delete_task_after_successful_execution() {
        execution.perform();

        verify(taskRepository).delete(task);
    }

    @Test
    void unlock_task_and_set_next_planned_execution_time_if_one_is_available() {
        LocalDateTime nextPlannedExecutionTime = systemClock.now().plusDays(1);
        when(oneTimeSchedule.nextExecutionTime(any())).thenReturn(Optional.of(nextPlannedExecutionTime));

        execution.perform();

        verify(taskRepository).unlock(task, nextPlannedExecutionTime);
        verify(taskRepository, never()).delete(any());
    }

    @Test
    void save_failure_on_exception() {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(runnable).accept(any());

        execution.perform();

        verify(taskRepository).saveFailure(task, exception);
    }

    @Test
    void notify_listeners_after_failed_execution() {
        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(runnable).accept(any());

        execution.perform();

        verify(listener).failed(taskName, taskId, parameter, exception);
    }

}
