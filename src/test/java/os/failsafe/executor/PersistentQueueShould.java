package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
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
    private PersistentTaskRepository persistentTaskRepository;
    private PersistentQueue persistentQueue;

    @BeforeEach
    void init() {
        systemClock = new TestSystemClock();
        persistentTaskRepository = Mockito.mock(PersistentTaskRepository.class);
        persistentQueue = new PersistentQueue(persistentTaskRepository, systemClock, lockTimeout);
    }

    @Test
    void add_a_task_to_repository() {
        when(persistentTaskRepository.add(any())).thenReturn(Mockito.mock(Task.class));

        LocalDateTime plannedExecutionTime = systemClock.now();
        Task task = createTask(plannedExecutionTime);

        persistentQueue.add(task);

        verify(persistentTaskRepository).add(task);
    }

    @Test
    void return_null_if_no_task_exists() {
        when(persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), anyInt())).thenReturn(Collections.emptyList());

        assertNull(persistentQueue.peekAndLock());
    }

    @Test
    void peek_and_lock_next_task() {
        Task task = Mockito.mock(Task.class);

        when(persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), anyInt())).thenReturn(Collections.singletonList(task));
        when(persistentTaskRepository.lock(task)).thenReturn(task);

        Task nextTask = persistentQueue.peekAndLock();

        verify(persistentTaskRepository).lock(task);
        assertEquals(task, nextTask);
    }

    @Test
    void find_next_tasks_for_execution_if_tasks_of_first_result_list_cannot_be_locked() {
        Task alreadyLocked = Mockito.mock(Task.class);
        Task toLock = Mockito.mock(Task.class);

        when(persistentTaskRepository.lock(alreadyLocked)).thenReturn(null);
        when(persistentTaskRepository.lock(toLock)).thenReturn(toLock);

        when(persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), anyInt())).thenReturn(Arrays.asList(alreadyLocked, alreadyLocked, alreadyLocked), Collections.singletonList(toLock));

        Task nextTask = persistentQueue.peekAndLock();

        verify(persistentTaskRepository).lock(toLock);
        assertEquals(toLock, nextTask);
    }

    @Test
    void return_null_if_first_result_list_cannot_be_locked_and_no_more_results_can_be_found() {
        Task alreadyLocked = Mockito.mock(Task.class);

        when(persistentTaskRepository.lock(alreadyLocked)).thenReturn(null);

        when(persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(any(), any(), anyInt())).thenReturn(Arrays.asList(alreadyLocked, alreadyLocked, alreadyLocked), Collections.emptyList());

        assertNull(persistentQueue.peekAndLock());
    }

    private Task createTask(LocalDateTime plannedExecutionTime) {
        return new Task(UUID.randomUUID().toString(), "Hello World!", "TaskName", plannedExecutionTime);
    }
}
