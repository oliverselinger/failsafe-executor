package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.task.FailedTask;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class FailedTaskShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private PersistentTasks persistentTasks;

    @BeforeEach
    void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        persistentTasks = new PersistentTasks(database, systemClock);
    }

    @Test
    void
    on_retry_clear_failed_status_and_unlock_itself() {
        PersistentTask persistentTask = createTask();
        RuntimeException exception = new RuntimeException("Sorry");
        persistentTask.fail(exception);

        FailedTask failedTask = persistentTasks.failedTask(persistentTask.getId()).orElseThrow(() -> new RuntimeException("Should be present"));

        failedTask.retry();

        Optional<FailedTask> task = persistentTasks.failedTask(persistentTask.getId());
        assertFalse(task.isPresent());
    }

    @Test
    void
    remove_itself_on_cancel() {
        PersistentTask persistentTask = createTask();
        RuntimeException exception = new RuntimeException("Sorry");
        persistentTask.fail(exception);

        FailedTask failedTask = persistentTasks.failedTask(persistentTask.getId()).orElseThrow(() -> new RuntimeException("Should be present"));

        failedTask.cancel();

        PersistentTask task = persistentTasks.findOne(persistentTask.getId());
        assertNull(task);
    }

    private PersistentTask createTask() {
        return persistentTasks.create(new TaskInstance("TestTask", "parameter", systemClock.now()));
    }
}
