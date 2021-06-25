package os.failsafe.executor.utils.testing;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitableTaskExecutionListenerShould {

    @Test
    void not_throw_an_exception_if_no_parties_have_registered() {
        assertDoesNotThrow(() -> new AwaitableTaskExecutionListener(Duration.ofMinutes(1)).awaitAllTasks());
    }

    @Test
    void not_block_if_no_parties_have_registered() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofMinutes(1));

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        countDownLatch.await(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void block_on_persisted_task_and_throw_timeout() {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofNanos(1));
        listener.persisting("TaskName", "taskId", "parameter");

        RuntimeException thrown = assertThrows(RuntimeException.class, listener::awaitAllTasks);
        assertEquals("Only 0/1 tasks finished! Waiting for: {taskId=TaskName#parameter}", thrown.getMessage());
    }

    @Test
    void block_on_retrying_task_and_throw_timeout() {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofNanos(1));
        listener.retrying("TaskName", "taskId", "parameter");

        assertThrows(RuntimeException.class, listener::awaitAllTasks);
    }

    @Test
    void release_block_when_task_fails() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofSeconds(1));
        listener.persisting("TaskName", "taskId", "parameter");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        listener.failed("TaskName", "taskId", "parameter", new Exception());
        countDownLatch.await(1, TimeUnit.SECONDS);
    }

    @Test
    void release_block_when_retried_task_succeeds() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofSeconds(1));
        listener.retrying("TaskName", "taskId", "parameter");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        listener.succeeded("TaskName", "taskId", "parameter");
        countDownLatch.await(1, TimeUnit.SECONDS);
    }

    @Test
    void return_all_failed_tasks_by_id() {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofSeconds(1));
        listener.persisting("TaskName", "taskId", "parameter");
        listener.failed("TaskName", "taskId", "parameter", new Exception());

        assertTrue(listener.isAnyExecutionFailed());
        assertEquals(1, listener.failedTasksByIds().size());
        assertTrue(listener.failedTasksByIds().contains("taskId"));
    }

    @Test
    void not_wait_for_ignored_task() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofMinutes(1), (name, id, param) -> true);
        listener.persisting("TaskName", "taskId", "parameter");
        listener.retrying("TaskName", "taskId", "parameter");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        countDownLatch.await(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void block_until_even_if_first_task_registers_and_arrives_before_next_task_registers() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(Duration.ofSeconds(3));

        String firstTaskId = "firstTaskId";
        listener.persisting("Task1", firstTaskId, "parameter");
        listener.succeeded("Task1", firstTaskId, "parameter");

        String secondTaskId = "firstTaskId";
        listener.persisting("Task2", secondTaskId, "parameter");

        AtomicBoolean done = new AtomicBoolean(false);

        Thread thread = executeInThread(() -> {
            listener.awaitAllTasks();
            done.set(true);
        });

        Awaitility
                .await()
                .pollDelay(Durations.ONE_MILLISECOND)
                .timeout(Duration.ofSeconds(2))
                .until(() -> thread.getState() == Thread.State.TIMED_WAITING);

        assertFalse(done.get());

        listener.succeeded("Task2", secondTaskId, "parameter");

        Awaitility
                .await()
                .pollDelay(Durations.ONE_MILLISECOND)
                .timeout(Duration.ofSeconds(2))
                .until(done::get);
    }

    private Thread executeInThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.start();
        return thread;
    }
}
