package os.failsafe.executor.utils.testing;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.FailsafeExecutor;
import os.failsafe.executor.Task;
import os.failsafe.executor.TaskExecutionListener;
import os.failsafe.executor.utils.Throwing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static os.failsafe.executor.utils.testing.FailsafeExecutorTestUtility.awaitAllTasks;

class FailsafeExecutorTestUtilityShould {

    @Test
    void callback_when_any_task_failed() throws Exception {
        FailsafeExecutor failsafeExecutor = Mockito.mock(FailsafeExecutor.class);

        doAnswer(invocation -> {
            TaskExecutionListener listener = invocation.getArgument(0, TaskExecutionListener.class);
            listener.failed("TaskName", "TaskId", "TaskParameter", new Exception());
            return null;
        }).when(failsafeExecutor).subscribe(any());

        Task failedTask = mock(Task.class);
        when(failsafeExecutor.findOne("TaskId")).thenReturn(java.util.Optional.ofNullable(failedTask));

        List<Task> actualFailedTasks = new ArrayList<>();
        awaitAllTasks(failsafeExecutor, () -> {
        }, actualFailedTasks::addAll);

        assertEquals(1, actualFailedTasks.size());
        assertTrue(actualFailedTasks.contains(failedTask));
    }

    @Test
    void callback_when_any_task_failed_different_api() throws Exception {
        FailsafeExecutor failsafeExecutor = Mockito.mock(FailsafeExecutor.class);

        doAnswer(invocation -> {
            TaskExecutionListener listener = invocation.getArgument(0, TaskExecutionListener.class);
            listener.failed("TaskName", "TaskId", "TaskParameter", new Exception());
            return null;
        }).when(failsafeExecutor).subscribe(any());

        Task failedTask = mock(Task.class);
        when(failsafeExecutor.findOne("TaskId")).thenReturn(java.util.Optional.ofNullable(failedTask));

        List<Task> actualFailedTasks = new ArrayList<>();
        awaitAllTasks(failsafeExecutor, Duration.ofSeconds(1), (Throwing.Runnable)(() -> {
        }), actualFailedTasks::addAll);

        assertEquals(1, actualFailedTasks.size());
        assertTrue(actualFailedTasks.contains(failedTask));
    }

    @Test
    void callback_when_any_task_failed_different_api_again() throws Exception {
        FailsafeExecutor failsafeExecutor = Mockito.mock(FailsafeExecutor.class);

        doAnswer(invocation -> {
            TaskExecutionListener listener = invocation.getArgument(0, TaskExecutionListener.class);
            listener.failed("TaskName", "TaskId", "TaskParameter", new Exception());
            return null;
        }).when(failsafeExecutor).subscribe(any());

        Task failedTask = mock(Task.class);
        when(failsafeExecutor.findOne("TaskId")).thenReturn(java.util.Optional.ofNullable(failedTask));

        List<Task> actualFailedTasks = new ArrayList<>();
        awaitAllTasks(failsafeExecutor, Duration.ofSeconds(1), (Throwing.Runnable)(() -> {
        }), actualFailedTasks::addAll, (name,  taskId,  parameter) -> true);

        assertEquals(1, actualFailedTasks.size());
        assertTrue(actualFailedTasks.contains(failedTask));
    }
}
