package os.failsafe.executor;

import org.junit.jupiter.api.Test;
import os.failsafe.executor.task.PersistentTask;
import os.failsafe.executor.task.TaskId;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class PersistentTaskShould {

    @Test
    void return_false_on_cancel_if_task_is_not_cancelable_because_it_is_locked() {
        PersistentTask persistentTask = new PersistentTask(new TaskId("id"), "parameter", "name", LocalDateTime.now(), LocalDateTime.now(), null, 0L, null);

        assertFalse(persistentTask.cancel());
    }

    @Test
    void return_false_on_retry_if_task_is_not_retryable() {
        PersistentTask persistentTask = new PersistentTask(new TaskId("id"), "parameter", "name", LocalDateTime.now(), null, null, 0L, null);

        assertFalse(persistentTask.retry());
    }

    private PersistentTask create() {
        return new PersistentTask("id", "parameter", "name", LocalDateTime.now());
    }
}
