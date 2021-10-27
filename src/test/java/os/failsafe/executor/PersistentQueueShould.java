package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PersistentQueueShould {

    private TestSystemClock systemClock = new TestSystemClock();
    private Duration lockTimeout = Duration.ofMinutes(10);
    private TaskRepository taskRepository;
    private Connection connection;
    private Database database;
    private PersistentQueue persistentQueue;
    private Set<String> processableTasks;

    @BeforeEach
    void init() throws SQLException {
        systemClock = new TestSystemClock();
        taskRepository = Mockito.mock(TaskRepository.class);
        connection = Mockito.mock(Connection.class);
        database = Mockito.mock(Database.class);
        doAnswer(ans -> {
            Database.ConnectionConsumer connectionConsumer = (Database.ConnectionConsumer) ans.getArguments()[0];
            connectionConsumer.accept(connection);
            return null;
        }).when(database).transactionNoResult(any());
        doAnswer(ans -> {
            Function connectionConsumer = (Function) ans.getArguments()[0];
            return connectionConsumer.apply(connection);
        }).when(database).transaction(any());
        doAnswer(ans -> {
            Function connectionConsumer = (Function) ans.getArguments()[0];
            return connectionConsumer.apply(connection);
        }).when(database).connect(any());

        persistentQueue = new PersistentQueue(database, taskRepository, systemClock, lockTimeout);
        processableTasks = new HashSet<>();
        processableTasks.add("Task");
    }

    @Test
    void add_a_task_to_repository() {
        when(taskRepository.add(any())).thenReturn(Mockito.mock(Task.class));

        LocalDateTime plannedExecutionTime = systemClock.now();
        Task task = createTask(plannedExecutionTime);

        persistentQueue.add(task);

        verify(taskRepository).add(task);
    }

    @Test
    void return_null_if_no_task_exists() {
        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(Collections.emptyList());

        assertEquals(0, persistentQueue.peekAndLock(processableTasks, 3).size());
    }

    @Test
    void peek_and_lock_next_task() {
        Task task = Mockito.mock(Task.class);

        List<Task> taskList = Collections.singletonList(task);
        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(taskList);
        when(taskRepository.lock(any(), any())).thenReturn(taskList);

        List<Task> nextTasks = persistentQueue.peekAndLock(processableTasks, 3);
        assertEquals(1, nextTasks.size());

        verify(taskRepository).lock(any(), eq(taskList));
        assertEquals(task, nextTasks.get(0));
    }

    @Test
    void return_empty_list_if_first_result_list_cannot_be_locked_and_no_more_results_can_be_found() {
        Task alreadyLocked = Mockito.mock(Task.class);

        when(taskRepository.lock(any(), any())).thenReturn(Collections.emptyList());

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(Arrays.asList(alreadyLocked, alreadyLocked, alreadyLocked));

        assertEquals(0, persistentQueue.peekAndLock(processableTasks, 3).size());
    }

    @Test
    void call_the_observer_and_pass_actual_query_result_zero() {
        PersistentQueueObserver observer = Mockito.mock(PersistentQueueObserver.class);
        persistentQueue.setObserver(observer);

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(Collections.emptyList());

        persistentQueue.peekAndLock(processableTasks, 3);
        verify(observer).onPeek(3, 0, 0);
    }

    @Test
    void call_the_observer_and_pass_actual_query_result_found_and_locked() {
        PersistentQueueObserver observer = Mockito.mock(PersistentQueueObserver.class);
        persistentQueue.setObserver(observer);

        Task alreadyLocked = Mockito.mock(Task.class);
        Task toLock = Mockito.mock(Task.class);
        List<Task> taskList = Arrays.asList(alreadyLocked, toLock);
        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(taskList);
        when(taskRepository.lock(any(), eq(taskList))).thenReturn(Collections.singletonList(toLock));

        persistentQueue.peekAndLock(processableTasks, 3);
        verify(observer).onPeek(3, 2, 1);
    }

    @Test
    void remove_the_observer_on_demand() {
        PersistentQueueObserver observer = Mockito.mock(PersistentQueueObserver.class);
        persistentQueue.setObserver(observer);

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), any(), anyInt())).thenReturn(Collections.emptyList());
        persistentQueue.peekAndLock(processableTasks, 3);
        verify(observer).onPeek(3, 0, 0);

        persistentQueue.setObserver(null);
        Mockito.reset(observer);
        persistentQueue.peekAndLock(processableTasks, 3);
        verify(observer, never()).onPeek(anyInt(), anyInt(), anyInt());
    }

    private Task createTask(LocalDateTime plannedExecutionTime) {
        return new Task(UUID.randomUUID().toString(), "TaskName", "Hello World!", plannedExecutionTime);
    }
}
