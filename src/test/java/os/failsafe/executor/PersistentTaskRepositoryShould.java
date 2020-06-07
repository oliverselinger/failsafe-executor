package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.task.ExecutionFailure;
import os.failsafe.executor.task.PersistentTask;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistentTaskRepositoryShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private PersistentTaskRepository persistentTaskRepository;
    private String taskName = "TestTask";
    private final String taskParameter = "parameter";
    private final LocalDateTime plannedExecutionTime = systemClock.now();

    @BeforeEach
    void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        persistentTaskRepository = new PersistentTaskRepository(database, systemClock);
    }

    @Test
    void add_a_task() {
        PersistentTask task = addTask();

        PersistentTask actual = persistentTaskRepository.findOne(task.getId());
        assertDoesNotThrow(() -> UUID.fromString(actual.getId().id));
        assertEquals(task.getId(), actual.getId());
        assertEquals(taskName, actual.getName());
        assertEquals(taskParameter, actual.getParameter());
        assertEquals(0L, actual.getVersion());
        assertEquals(plannedExecutionTime, actual.getPlannedExecutionTime());
        assertNull(actual.getLockTime());
    }

    @Test
    void add_a_task_with_given_id() {
        String id = "id";
        PersistentTask task = addTask(id);

        PersistentTask actual = persistentTaskRepository.findOne(task.getId());
        assertEquals(id, task.getId().id);
        assertEquals(id, actual.getId().id);
    }

    @Test
    void do_nothing_if_id_is_not_unique() {
        String id = "id";
        addTask(id);
        assertDoesNotThrow(() -> addTask(id));
    }

    @Test
    void find_and_return_all_tasks() {
        PersistentTask task1 = addTask();
        PersistentTask task2 = addTask();

        List<PersistentTask> tasks = persistentTaskRepository.findAll();
        assertEquals(2, tasks.size());
        assertTrue(tasks.containsAll(Arrays.asList(task1, task2)));
    }

    @Test
    void find_and_return_empty_list_if_no_task_exists() {
        assertTrue(persistentTaskRepository.findAll().isEmpty());
    }

    @Test
    void return_empty_list_if_no_unlocked_task_exists() {
        assertEquals(0, persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now(), systemClock.now().minusMinutes(10), 3).size());
    }

    @Test
    void return_empty_list_if_tasks_planned_execution_time_is_not_reached_yet() {
        addTask();

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now().minusMinutes(1), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_task_if_planned_execution_time_is_reached() {
        PersistentTask task = addTask();

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(1, tasks.size());
        assertEquals(task.getId(), tasks.get(0).getId());
    }

    @Test
    void return_tasks_ordered_by_created_date() {
        PersistentTask task1 = addTask();
        systemClock.timeTravelBy(Duration.ofMillis(1)); // next added task could get same timestamp because it is too fast
        PersistentTask task2 = addTask();
        systemClock.timeTravelBy(Duration.ofMillis(1));
        PersistentTask task3 = addTask();

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(task1.getId(), tasks.get(0).getId());
        assertEquals(task2.getId(), tasks.get(1).getId());
        assertEquals(task3.getId(), tasks.get(2).getId());
    }

    @Test
    void lock_a_task() {
        PersistentTask task = addTask();

        LocalDateTime lockTime = LocalDateTime.now();
        systemClock.fixedTime(lockTime);

        PersistentTask locked = persistentTaskRepository.lock(task);

        assertEquals(lockTime, locked.getLockTime());
        assertTrue(locked.isLocked());
    }

    @Test
    void
    unlock_a_task_and_set_next_planned_execution_time() {
        PersistentTask task = addTask();

        PersistentTask locked = persistentTaskRepository.lock(task);

        LocalDateTime nextPlannedExecutionTime = systemClock.now().plusDays(1);
        persistentTaskRepository.unlock(locked, nextPlannedExecutionTime);

        PersistentTask unlocked = persistentTaskRepository.findOne(task.getId());

        assertNull(unlocked.getLockTime());
        assertFalse(unlocked.isLocked());
        assertEquals(nextPlannedExecutionTime, unlocked.getPlannedExecutionTime());
    }

    @Test
    void never_return_a_locked_task_before_lock_timeout() {
        PersistentTask task = addTask();

        persistentTaskRepository.lock(task);

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_a_locked_task_after_lock_timeout_is_reached() {
        PersistentTask task = addTask();

        persistentTaskRepository.lock(task);

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now().plusDays(1), systemClock.now(), 3);

        assertEquals(1, tasks.size());
        assertEquals(task.getId(), tasks.get(0).getId());
    }

    @Test
    void save_an_execution_failure() {
        PersistentTask task = addTask();

        String exceptionMessage = "Exception message";
        Exception exception = new Exception(exceptionMessage);

        persistentTaskRepository.saveFailure(task, exception);

        List<PersistentTask> failedTasks = persistentTaskRepository.findAllFailedTasks();
        assertEquals(1, failedTasks.size());

        PersistentTask failedTask = failedTasks.get(0);
        assertNull(failedTask.getLockTime());

        ExecutionFailure executionFailure = failedTask.getExecutionFailure();
        assertNotNull(executionFailure);
        assertNotNull(executionFailure.getFailTime());
        assertEquals(exceptionMessage, executionFailure.getExceptionMessage());
        assertEquals(ExceptionUtils.stackTraceAsString(exception), executionFailure.getStackTrace());
    }

    @Test
    void never_return_a_failed_task() {
        PersistentTask task = addTask();

        persistentTaskRepository.saveFailure(task, new Exception());

        List<PersistentTask> tasks = persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_empty_list_if_no_failed_task_exists() {
        addTask();

        List<PersistentTask> failedTasks = persistentTaskRepository.findAllFailedTasks();
        assertTrue(failedTasks.isEmpty());
    }

    @Test
    void delete_a_task() {
        PersistentTask persistentTask = addTask();

        persistentTaskRepository.delete(persistentTask);

        assertNull(persistentTaskRepository.findOne(persistentTask.getId()));
    }

    @Test
    void delete_failure_of_a_task() {
        PersistentTask task = addTask();

        RuntimeException exception = new RuntimeException("Sorry");
        persistentTaskRepository.saveFailure(task, exception);


        PersistentTask failedTask = persistentTaskRepository.findAllFailedTasks().get(0);
        persistentTaskRepository.deleteFailure(failedTask);

        List<PersistentTask> allFailedTasks = persistentTaskRepository.findAllFailedTasks();
        assertTrue(allFailedTasks.isEmpty());

        PersistentTask taskWithoutFailure = persistentTaskRepository.findOne(task.getId());
        assertNull(taskWithoutFailure.getExecutionFailure());
        assertFalse(taskWithoutFailure.isExecutionFailed());
    }

    private PersistentTask addTask() {
        return persistentTaskRepository.add(new TaskInstance(taskName, taskParameter, plannedExecutionTime));
    }

    private PersistentTask addTask(String id) {
        return persistentTaskRepository.add(new TaskInstance(id, taskName, taskParameter, plannedExecutionTime));
    }
}
