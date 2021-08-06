package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_TABLE_NAME;

class TaskRepositoryShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private TaskRepository taskRepository;
    private String taskName = "TestTask";
    private final String taskParameter = "parameter";
    private final LocalDateTime plannedExecutionTime = systemClock.now();
    private Set<String> processableTasks;

    @BeforeEach
    void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        taskRepository = new TaskRepository(database, DEFAULT_TABLE_NAME, systemClock);
        processableTasks = new HashSet<>();
        processableTasks.add(taskName);
    }

    @Test
    void add_a_task() {
        Task task = addTask();

        Task actual = taskRepository.findOne(task.getId());
        assertDoesNotThrow(() -> UUID.fromString(actual.getId()));
        assertEquals(task.getId(), actual.getId());
        assertEquals(taskName, actual.getName());
        assertEquals(taskParameter, actual.getParameter());
        assertEquals(0, actual.getRetryCount());
        assertEquals(0L, actual.getVersion());
        assertNotNull(actual.getCreationTime());
        assertEquals(plannedExecutionTime, actual.getPlannedExecutionTime());
        assertNull(actual.getLockTime());
    }

    @Test
    void add_a_task_with_given_id() {
        String id = "id";
        Task task = addTask(id);

        Task actual = taskRepository.findOne(task.getId());
        assertEquals(id, task.getId());
        assertEquals(id, actual.getId());
    }

    @Test
    void do_nothing_if_id_is_not_unique() {
        String id = "id";
        addTask(id);
        assertDoesNotThrow(() -> addTask(id));
    }

    @Test
    void find_and_return_all_tasks() {
        Task task1 = addTask();
        Task task2 = addTask();

        List<Task> tasks = taskRepository.findAll();
        assertEquals(2, tasks.size());
        assertTrue(tasks.containsAll(Arrays.asList(task1, task2)));
    }

    @Test
    void find_and_return_empty_list_if_no_task_exists() {
        assertTrue(taskRepository.findAll().isEmpty());
    }

    @Test
    void return_empty_list_if_no_unlocked_task_exists() {
        assertEquals(0, taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3).size());
    }

    @Test
    void return_empty_list_if_tasks_planned_execution_time_is_not_reached_yet() {
        addTask();

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now().minusMinutes(1), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_task_if_planned_execution_time_is_reached() {
        Task task = addTask();

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(1, tasks.size());
        assertEquals(task.getId(), tasks.get(0).getId());
    }

    @Test
    void return_tasks_ordered_by_created_date() {
        Task task1 = addTask(systemClock.now());
        systemClock.timeTravelBy(Duration.ofMillis(1)); // next added task could get same timestamp because it is too fast
        Task task2 = addTask(systemClock.now());
        systemClock.timeTravelBy(Duration.ofMillis(1));
        Task task3 = addTask(systemClock.now());

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(task1.getId(), tasks.get(0).getId());
        assertEquals(task2.getId(), tasks.get(1).getId());
        assertEquals(task3.getId(), tasks.get(2).getId());
    }

    @Test
    void lock_a_task() {
        Task task = addTask();

        LocalDateTime lockTime = systemClock.now();
        systemClock.fixedTime(lockTime);

        Task locked = taskRepository.lock(task);

        assertEquals(lockTime, locked.getLockTime());
        assertTrue(locked.isLocked());
    }

    @Test
    void
    unlock_a_task_and_set_next_planned_execution_time() {
        Task task = addTask();

        Task locked = taskRepository.lock(task);

        LocalDateTime nextPlannedExecutionTime = systemClock.now().plusDays(1);
        taskRepository.unlock(locked, nextPlannedExecutionTime);

        Task unlocked = taskRepository.findOne(task.getId());

        assertNull(unlocked.getLockTime());
        assertFalse(unlocked.isLocked());
        assertEquals(nextPlannedExecutionTime, unlocked.getPlannedExecutionTime());
    }

    @Test
    void never_return_a_locked_task_before_lock_timeout() {
        Task task = addTask();

        taskRepository.lock(task);

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_a_locked_task_after_lock_timeout_is_reached() {
        Task task = addTask();

        taskRepository.lock(task);

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now().plusDays(1), systemClock.now(), 3);

        assertEquals(1, tasks.size());
        assertEquals(task.getId(), tasks.get(0).getId());
    }

    @Test
    void save_an_execution_failure() {
        Task task = addTask();

        String exceptionMessage = "Exception message";
        Exception exception = new Exception(exceptionMessage);

        taskRepository.saveFailure(task, exception);

        List<Task> failedTasks = taskRepository.findAllFailedTasks();
        assertEquals(1, failedTasks.size());

        Task failedTask = failedTasks.get(0);
        assertNull(failedTask.getLockTime());

        ExecutionFailure executionFailure = failedTask.getExecutionFailure();
        assertNotNull(executionFailure);
        assertNotNull(executionFailure.getFailTime());
        assertEquals(exceptionMessage, executionFailure.getExceptionMessage());
        assertEquals(ExceptionUtils.stackTraceAsString(exception), executionFailure.getStackTrace());
    }

    @Test
    void never_return_a_failed_task() {
        Task task = addTask();

        taskRepository.saveFailure(task, new Exception());

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);

        assertEquals(0, tasks.size());
    }

    @Test
    void return_empty_list_if_no_failed_task_exists() {
        addTask();

        List<Task> failedTasks = taskRepository.findAllFailedTasks();
        assertTrue(failedTasks.isEmpty());
    }

    @Test
    void delete_a_task() {
        Task task = addTask();

        taskRepository.delete(task);

        assertNull(taskRepository.findOne(task.getId()));
    }

    @Test
    void delete_failure_of_a_task_and_increase_retry_count() {
        Task task = addTask();

        RuntimeException exception = new RuntimeException("Sorry");
        taskRepository.saveFailure(task, exception);

        Task failedTask = taskRepository.findAllFailedTasks().get(0);
        taskRepository.deleteFailure(failedTask);

        List<Task> allFailedTasks = taskRepository.findAllFailedTasks();
        assertTrue(allFailedTasks.isEmpty());

        Task taskWithoutFailure = taskRepository.findOne(task.getId());
        assertNull(taskWithoutFailure.getExecutionFailure());
        assertFalse(taskWithoutFailure.isExecutionFailed());
        assertEquals(1, taskWithoutFailure.getRetryCount());
    }

    @Test
    void find_only_registered_tasks() {
        addTask();
        addTask();
        addTask("OtherTaskName", plannedExecutionTime);
        processableTasks.add("Some other");

        List<Task> tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);
        assertEquals(tasks.size(), 2);

        tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(Collections.emptySet(), systemClock.now(), systemClock.now().minusMinutes(10), 3);
        assertEquals(tasks.size(), 0);

        Set<String> unknownTasks = new HashSet<>();
        unknownTasks.add("UnknownTaskName");

        tasks = taskRepository.findAllNotLockedOrderedByCreatedDate(unknownTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3);
        assertEquals(tasks.size(), 0);

        assertEquals(taskRepository.findAll().size(), 3);

    }

    private Task addTask() {
        return addTask(UUID.randomUUID().toString());
    }

    private Task addTask(String id) {
        String taskName = this.taskName;
        LocalDateTime plannedExecutionTime = this.plannedExecutionTime;
        return addTask(id, taskName, plannedExecutionTime);
    }

    private Task addTask(LocalDateTime plannedExecutionTime) {
        return addTask(UUID.randomUUID().toString(), taskName, plannedExecutionTime);
    }

    private Task addTask(String taskName, LocalDateTime plannedExecutionTime) {
        return addTask(UUID.randomUUID().toString(), taskName, plannedExecutionTime);
    }

    private Task addTask(String id, String taskName, LocalDateTime plannedExecutionTime) {
        return taskRepository.add(new Task(id, taskName, taskParameter, plannedExecutionTime));
    }
}
