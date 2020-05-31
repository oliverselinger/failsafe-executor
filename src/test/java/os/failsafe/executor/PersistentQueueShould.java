package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PersistentQueueShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private TestSystemClock systemClock = new TestSystemClock();
    private Duration lockTimeout = Duration.ofMinutes(10);
    private PersistentQueue persistentQueue;

    @BeforeEach
    void init() {
        systemClock = new TestSystemClock();
        persistentQueue = new PersistentQueue(DB_EXTENSION.database(), systemClock, lockTimeout);
    }

    @Test void
    return_null_if_queue_is_empty() {
        assertNull(persistentQueue.peekAndLock());
    }

    @Test void
    store_and_enqueue_a_task() {
        LocalDateTime plannedExecutionTime = systemClock.now();
        TaskInstance task = createTask(plannedExecutionTime);

        TaskId persistentTask = persistentQueue.add(task);

        List<PersistentTask> all = persistentQueue.allQueued();
        assertEquals(1, all.size());

        PersistentTask actual = all.get(0);
        assertEquals(task.name, actual.getName());
        assertEquals(task.parameter, actual.getParameter());
        assertEquals(plannedExecutionTime, actual.getPlannedExecutionTime());
        assertEquals(persistentTask, actual.getId());
    }

    @Test void
    peek_tasks_in_fifo_sequence() {
        TaskInstance task1 = createTask();
        TaskInstance task2 = createTask();
        TaskInstance task3 = createTask();

        TaskId persistentTask1 = persistentQueue.add(task1);
        systemClock.timeTravelBy(Duration.ofMillis(1)); // next added task could get same timestamp because it is too fast
        TaskId persistentTask2 = persistentQueue.add(task2);
        systemClock.timeTravelBy(Duration.ofMillis(1));
        TaskId persistentTask3 =  persistentQueue.add(task3);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();
        PersistentTask dequedTask2 = persistentQueue.peekAndLock();
        PersistentTask dequedTask3 = persistentQueue.peekAndLock();

        assertEquals(persistentTask1, dequedTask1.getId());
        assertEquals(persistentTask2, dequedTask2.getId());
        assertEquals(persistentTask3, dequedTask3.getId());
    }

    @Test void
    peek_a_task_again_after_lock_times_out() {
        TaskInstance task = createTask();
        persistentQueue.add(task);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();

        systemClock.timeTravelBy(lockTimeout);

        PersistentTask dequedTask2 = persistentQueue.peekAndLock();

        assertNotNull(dequedTask1);
        assertNotNull(dequedTask2);
        assertEquals(dequedTask1.getId(), dequedTask2.getId());
    }

    @Test void
    never_peek_a_failed_task() {
        TaskInstance task = createTask();
        persistentQueue.add(task);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();

        dequedTask1.fail(new RuntimeException());

        systemClock.timeTravelBy(lockTimeout);

        PersistentTask dequedTask2 = persistentQueue.peekAndLock();

        assertNotNull(dequedTask1);
        assertNull(dequedTask2);
    }

    @Test void
    never_peek_a_planned_future_task() {
        TaskInstance task = createTask(systemClock.now().plusDays(1));
        persistentQueue.add(task);

        PersistentTask peekedTask = persistentQueue.peekAndLock();
        assertNull(peekedTask);
    }

    @Test void
    store_and_enqueue_a_task_with_future_execution() {
        TaskInstance task = createTask(systemClock.now().plusDays(1));

        persistentQueue.add(task);

        List<PersistentTask> all = persistentQueue.allQueued();
        assertEquals(1, all.size());

        PersistentTask actual = all.get(0);
        assertEquals(task.name, actual.getName());
    }

    private TaskInstance createTask() {
        return createTask(systemClock.now());
    }

    private TaskInstance createTask(LocalDateTime plannedExecutionTime) {
        return new TaskInstance("TaskName", "Hello world!", plannedExecutionTime);
    }
}
