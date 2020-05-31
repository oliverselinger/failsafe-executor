package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.task.FailedTask;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentTasksShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private PersistentTasks persistentTasks;
    private String taskName = "TestTask";
    private final String taskParameter = "parameter";
    private final LocalDateTime plannedExecutionTime = systemClock.now();

    @BeforeEach
    void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        persistentTasks = new PersistentTasks(database, systemClock);
    }

    @Test
    void
    persist_and_find_a_task() {
        PersistentTask task = createTask();

        PersistentTask actual = persistentTasks.findOne(task.getId());
        assertDoesNotThrow(() -> UUID.fromString(actual.getId().id));
        assertEquals(task.getId(), actual.getId());
        assertEquals(taskName, actual.getName());
        assertEquals(taskParameter, actual.getParameter());
        assertEquals(0L, actual.version);
        assertEquals(plannedExecutionTime, actual.getPlannedExecutionTime());
        assertNull(actual.startTime);
    }

    @Test
    void
    persist_a_task_with_given_id() {
        String id = "id";
        PersistentTask task = createTask(id);

        PersistentTask actual = persistentTasks.findOne(task.getId());
        assertEquals(id, task.getId().id);
        assertEquals(id, actual.getId().id);
    }

    @Test
    void
    do_nothing_if_id_is_not_unique() {
        String id = "id";
        createTask(id);
        assertDoesNotThrow(() -> createTask(id));
    }

    @Test
    void
    return_empty_list_if_no_task_is_failed() {
        createTask();

        List<FailedTask> failedTasks = persistentTasks.failedTasks();
        assertTrue(failedTasks.isEmpty());
    }

    @Test
    void
    return_empty_optional_if_task_is_not_failed() {
        PersistentTask persistentTask = createTask();

        Optional<FailedTask> failedTasks = persistentTasks.failedTask(persistentTask.getId());
        assertFalse(failedTasks.isPresent());
    }

    private PersistentTask createTask() {
        return persistentTasks.create(new TaskInstance(taskName, taskParameter, plannedExecutionTime));
    }

    private PersistentTask createTask(String id) {
        return persistentTasks.create(new TaskInstance(id, taskName, taskParameter, plannedExecutionTime));
    }
}
