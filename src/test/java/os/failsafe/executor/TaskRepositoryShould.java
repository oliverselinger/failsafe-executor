package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.TestSystemClock;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        Task task1 = addTask("1");
        systemClock.timeTravelBy(Duration.ofSeconds(1));
        Task task2 = addTask("2");
        systemClock.timeTravelBy(Duration.ofSeconds(1));
        Task task3 = addTask("3");

        // test query params
        assertFindOne("2", task2);
        // single
        assertFindAll(taskName, null, null, task1, task2, task3);
        assertFindAll("notExisting", null, null);
        assertFindAll(null, taskParameter, null, task1, task2, task3);
        assertFindAll(null, "notExisting", null);
        assertFindAll(null, null, false, task1, task2, task3);
        assertFindAll(null, null, true);
        // combinations
        assertFindAll(taskName, taskParameter, null, task1, task2, task3);
        assertFindAll("notExisting", taskParameter, null);
        assertFindAll(taskName, "notExisting", null);
        assertFindAll(taskName, taskParameter, false, task1, task2, task3);
        assertFindAll(taskName, taskParameter, true);

        // test paging
        assertFindAll(0, 2, task3, task2);
        assertFindAll(2, 100, task1);
        assertFindAll(3, 100);

        // test sorting
        assertFindAll(new Sort(Sort.Field.CREATED_DATE, Sort.Direction.ASC), task1, task2, task3);
        assertFindAll(new Sort(Sort.Field.CREATED_DATE, Sort.Direction.DESC), task3, task2, task1);
        assertFindAll(new Sort(Sort.Field.ID, Sort.Direction.ASC), task1, task2, task3);
        assertFindAll(new Sort(Sort.Field.ID, Sort.Direction.DESC), task3, task2, task1);

        ArrayList<Sort> sorts = new ArrayList<>();
        sorts.add(new Sort(Sort.Field.CREATED_DATE, Sort.Direction.ASC));
        sorts.add(new Sort(Sort.Field.ID, Sort.Direction.ASC));
        assertFindAll(sorts, task1, task2, task3);
    }

    @Test
    void find_and_return_empty_list_if_no_task_exists() {
        assertFindAll(0, 100);
        assertFindAll(10, 100);
    }

    @Test
    void return_empty_list_if_no_unlocked_task_exists() {
        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));
        assertEquals(0, tasks.size());
    }

    @Test
    void return_empty_list_if_tasks_planned_execution_time_is_not_reached_yet() {
        addTask();

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now().minusMinutes(1), systemClock.now().minusMinutes(10), 3));

        assertEquals(0, tasks.size());
    }

    @Test
    void return_task_if_planned_execution_time_is_reached() {
        Task task = addTask();

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));

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

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));

        assertEquals(task1.getId(), tasks.get(0).getId());
        assertEquals(task2.getId(), tasks.get(1).getId());
        assertEquals(task3.getId(), tasks.get(2).getId());
    }

    @Test
    void lock_a_task() {
        Task task = addTask();

        LocalDateTime lockTime = systemClock.now();
        systemClock.fixedTime(lockTime);

        List<Task> locked = database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), null));
        assertEquals(1, locked.size());

        Task lockedTask = locked.get(0);
        assertEquals(lockTime, lockedTask.getLockTime());
        assertTrue(lockedTask.isLocked());
    }

    @Test
    void lock_a_task_with_a_nodeid(){
        Task task = addTask();

        String nodeId = String.valueOf(UUID.randomUUID());
        List<Task> locked = database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), nodeId));
        assertEquals(1, locked.size());

        Task lockedTask = locked.get(0);
        assertEquals(nodeId, lockedTask.getNodeId());
        assertTrue(lockedTask.isLocked());
    }

    @Test
    void only_lock_unlocked_tasks_by_using_optimistic_locking_strategy() {
        Task taskToLock1 = addTask();
        Task taskMeanwhileLockedByOtherNode = addTask();
        Task taskToLock2 = addTask();

        LocalDateTime lockTime = systemClock.now();
        systemClock.fixedTime(lockTime);

        database.connect(con -> taskRepository.lock(con, Collections.singletonList(taskMeanwhileLockedByOtherNode), null));

        List<Task> locked = database.connect(con -> taskRepository.lock(con, Arrays.asList(taskToLock1, taskMeanwhileLockedByOtherNode, taskToLock2), null));
        assertEquals(2, locked.size());

        Task lockedTask = locked.get(0);
        assertEquals(taskToLock1.getId(), lockedTask.getId());
        assertEquals(lockTime, lockedTask.getLockTime());
        assertTrue(lockedTask.isLocked());

        lockedTask = locked.get(1);
        assertEquals(taskToLock2.getId(), lockedTask.getId());
        assertEquals(lockTime, lockedTask.getLockTime());
        assertTrue(lockedTask.isLocked());
    }

    @Test
    void unlock_a_task_and_set_next_planned_execution_time() {
        Task task = addTask();

        List<Task> locked = database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), null));
        assertEquals(1, locked.size());

        LocalDateTime nextPlannedExecutionTime = systemClock.now().plusDays(1);
        taskRepository.unlock(locked.get(0), nextPlannedExecutionTime);

        Task unlocked = taskRepository.findOne(task.getId());

        assertNull(unlocked.getLockTime());
        assertFalse(unlocked.isLocked());
        assertEquals(nextPlannedExecutionTime, unlocked.getPlannedExecutionTime());
    }

    @Test
    void never_return_a_locked_task_before_lock_timeout() {
        Task task = addTask();

        database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), null));

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));

        assertEquals(0, tasks.size());
    }

    @Test
    void return_a_locked_task_after_lock_timeout_is_reached() {
        Task task = addTask();

        database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), null));

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now().plusDays(1), systemClock.now(), 3));

        assertEquals(1, tasks.size());
        assertEquals(task.getId(), tasks.get(0).getId());
    }

    @Test
    void save_an_execution_failure() {
        Task task = addTask();

        String exceptionMessage = "Exception message";
        Exception exception = new Exception(exceptionMessage);

        taskRepository.saveFailure(task, new ExecutionFailure(systemClock.now(), exception));

        List<Task> failedTasks = findAllFailedTasks();
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

        taskRepository.saveFailure(task, new ExecutionFailure(systemClock.now(), new Exception()));

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));

        assertEquals(0, tasks.size());
    }

    @Test
    void delete_a_task() {
        Task task = addTask();

        taskRepository.delete(task);

        assertNull(taskRepository.findOne(task.getId()));
    }

    @Test
    void delete_failure_of_a_task_and_increase_retry_count() throws SQLException {
        Task task = addTask();

        RuntimeException exception = new RuntimeException("Sorry");
        taskRepository.saveFailure(task, new ExecutionFailure(systemClock.now(), exception));

        Task failedTask = findAllFailedTasks().get(0);
        database.connectNoResult(con -> taskRepository.deleteFailure(con, failedTask));

        List<Task> allFailedTasks = findAllFailedTasks();
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

        List<Task> tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, processableTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));
        assertEquals(tasks.size(), 2);

        tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, Collections.emptySet(), systemClock.now(), systemClock.now().minusMinutes(10), 3));
        assertEquals(tasks.size(), 0);

        Set<String> unknownTasks = new HashSet<>();
        unknownTasks.add("UnknownTaskName");

        tasks = database.connect(con -> taskRepository.findAllNotLockedOrderedByCreatedDate(con, unknownTasks, systemClock.now(), systemClock.now().minusMinutes(10), 3));
        assertEquals(tasks.size(), 0);

        assertEquals(findAllTasks().size(), 3);
    }

    @Test
    void update_lock_time() {
        systemClock.fixedTime(LocalDateTime.of(2020, 11, 12, 10, 0));

        Task task = addTask();
        List<Task> lockedTasks = database.connect(con -> taskRepository.lock(con, Collections.singletonList(task), null));
        Task locked = lockedTasks.get(0);

        systemClock.timeTravelBy(Duration.ofSeconds(5));

        List<Task> tasks = taskRepository.updateLockTime(Collections.singletonList(locked));
        assertEquals(1, tasks.size());

        Task actual = tasks.get(0);
        assertEquals(locked.getLockTime().plusSeconds(5), actual.getLockTime());
    }

    @Test
    void not_update_lock_time_if_task_is_not_locked() {
        Task task = addTask();
        List<Task> tasks = taskRepository.updateLockTime(Collections.singletonList(task));
        assertTrue(tasks.isEmpty());

        Task actual = taskRepository.findOne(task.getId());
        assertNull(actual.getLockTime());
        assertEquals(task.getVersion(), actual.getVersion());
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
        Task task = new Task(id, taskName, taskParameter, plannedExecutionTime);
        taskRepository.add(task);
        return taskRepository.findOne(id);
    }

    private void assertFindAll(Sort sort, Task... expectedTasks) {
        assertFindAll(Collections.singletonList(sort), expectedTasks);
    }

    private void assertFindAll(List<Sort> sorts, Task... expectedTasks) {
        List<Task> tasks = taskRepository.findAll(null, null, null, 0, 100, sorts.toArray(new Sort[0]));

        assertEquals(expectedTasks.length, tasks.size());
        for (int i = 0; i < expectedTasks.length; i++) {
            Task expectedTask = expectedTasks[i];
            assertEquals(expectedTask, tasks.get(i));
        }
    }

    private void assertFindAll(int offset, int limit, Task... expectedTasks) {
        assertFindAll(null, null, null, offset, limit, expectedTasks);
    }

    private void assertFindAll(String taskName, String taskParameter, Boolean failed, Task... expectedTasks) {
        assertFindAll(taskName, taskParameter, failed, 0, 100, expectedTasks);
    }

    private void assertFindAll(String taskName, String taskParameter, Boolean failed, int offset, int limit, Task... expectedTasks) {
        List<Task> tasks = taskRepository.findAll(taskName, taskParameter, failed, offset, limit);

        assertEquals(expectedTasks.length, tasks.size());
        List<Task> expected = Arrays.asList(expectedTasks);
        assertTrue(tasks.containsAll(expected));
    }

    private void assertFindOne(String id, Task expected) {
        Task actual = taskRepository.findOne(id);
        assertEquals(expected, actual);
    }

    private List<Task> findAllTasks() {
        return taskRepository.findAll(null, null, null, 0, 100);
    }

    private List<Task> findAllFailedTasks() {
        return taskRepository.findAll(null, null, true, 0, 100);
    }
}
