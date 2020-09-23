package os.failsafe.executor;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TaskShould {

    @Test
    void return_false_on_cancel_if_task_is_not_cancelable_because_it_is_locked() {
        Task task = new Task("id", "parameter", "name", LocalDateTime.now(), LocalDateTime.now(), LocalDateTime.now(), null, 0L);

        assertFalse(task.isCancelable());
    }

    @Test
    void return_false_on_retry_if_task_is_not_retryable() {
        Task task = new Task("id", "parameter", "name", LocalDateTime.now(), LocalDateTime.now(), null, null, 0L);

        assertFalse(task.isRetryable());
    }

    @Test
    void print_its_internal_state() {
        LocalDateTime dateTime = LocalDateTime.of(2020, 5, 5, 10, 30);
        Task task = new Task("id", "parameter", "name", dateTime, dateTime, dateTime, new ExecutionFailure(dateTime, "exceptionMsg", "stackTrace"), 0L);

        assertEquals("Task{id='id', parameter='parameter', name='name', creationTime=2020-05-05T10:30, plannedExecutionTime=2020-05-05T10:30, lockTime=2020-05-05T10:30, executionFailure=ExecutionFailure{failTime=2020-05-05T10:30, exceptionMessage='exceptionMsg', stackTrace='stackTrace'}, version=0}",
                task.toString());
    }

}
