package os.failsafe.executor;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TaskShould {

    @Test
    void return_false_on_cancel_if_task_is_not_cancelable_because_it_is_locked() {
        Task task = new Task(new String("id"), "parameter", "name", LocalDateTime.now(), LocalDateTime.now(), null, 0L, null);

        assertFalse(task.cancel());
    }

    @Test
    void return_false_on_retry_if_task_is_not_retryable() {
        Task task = new Task(new String("id"), "parameter", "name", LocalDateTime.now(), null, null, 0L, null);

        assertFalse(task.retry());
    }

    private Task create() {
        return new Task("id", "parameter", "name", LocalDateTime.now());
    }
}
