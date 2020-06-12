package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PersistentQueueShould {

    private TestSystemClock systemClock = new TestSystemClock();
    private Duration lockTimeout = Duration.ofMinutes(10);
    private TaskRepository taskRepository;
    private PersistentQueue persistentQueue;
    private Set<String> processableTasks;

    @BeforeEach
    void init() {
        systemClock = new TestSystemClock();
        taskRepository = Mockito.mock(TaskRepository.class);
        persistentQueue = new PersistentQueue(taskRepository, systemClock, lockTimeout);
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
        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), anyInt())).thenReturn(Collections.emptyList());

        assertNull(persistentQueue.peekAndLock(processableTasks));
    }

    @Test
    void peek_and_lock_next_task() {
        Task task = Mockito.mock(Task.class);

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), anyInt())).thenReturn(Collections.singletonList(task));
        when(taskRepository.lock(task)).thenReturn(task);

        Task nextTask = persistentQueue.peekAndLock(processableTasks);

        verify(taskRepository).lock(task);
        assertEquals(task, nextTask);
    }

    @Test
    void find_next_tasks_for_execution_if_tasks_of_first_result_list_cannot_be_locked() {
        Task alreadyLocked = Mockito.mock(Task.class);
        Task toLock = Mockito.mock(Task.class);

        when(taskRepository.lock(alreadyLocked)).thenReturn(null);
        when(taskRepository.lock(toLock)).thenReturn(toLock);

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), anyInt())).thenReturn(Arrays.asList(alreadyLocked, alreadyLocked, alreadyLocked), Collections.singletonList(toLock));

        Task nextTask = persistentQueue.peekAndLock(processableTasks);

        verify(taskRepository).lock(toLock);
        assertEquals(toLock, nextTask);
    }

    @Test
    void return_null_if_first_result_list_cannot_be_locked_and_no_more_results_can_be_found() {
        Task alreadyLocked = Mockito.mock(Task.class);

        when(taskRepository.lock(alreadyLocked)).thenReturn(null);

        when(taskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), any(), anyInt())).thenReturn(Arrays.asList(alreadyLocked, alreadyLocked, alreadyLocked), Collections.emptyList());

        assertNull(persistentQueue.peekAndLock(processableTasks));
    }

    private Task createTask(LocalDateTime plannedExecutionTime) {
        return new Task(UUID.randomUUID().toString(), "Hello World!", "TaskName", plannedExecutionTime);
    }
}
