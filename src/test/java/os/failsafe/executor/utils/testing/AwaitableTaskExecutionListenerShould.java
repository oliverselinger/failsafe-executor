package os.failsafe.executor.utils.testing;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitableTaskExecutionListenerShould {

    @Test
    void not_throw_an_exception_if_no_parties_have_registered() {
        assertDoesNotThrow(() -> new AwaitableTaskExecutionListener(1, TimeUnit.MINUTES).awaitAllTasks());
    }

    @Test
    void not_block_if_no_parties_have_registered() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(1, TimeUnit.MINUTES);

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        countDownLatch.await(100, TimeUnit.MILLISECONDS);
    }

    @Test
    void throw_exception_on_timeout() {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(1, TimeUnit.NANOSECONDS);
        listener.registered("TaskName", "taskId", "parameter");

        assertThrows(RuntimeException.class, listener::awaitAllTasks);
    }

    @Test
    void release_block_when_task_fails() throws InterruptedException {
        AwaitableTaskExecutionListener listener = new AwaitableTaskExecutionListener(1, TimeUnit.SECONDS);
        listener.registered("TaskName", "taskId", "parameter");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        executeInThread(() -> {
            listener.awaitAllTasks();
            countDownLatch.countDown();
        });

        listener.failed("TaskName", "taskId", "parameter", new Exception());
        countDownLatch.await(1, TimeUnit.SECONDS);
    }

    private void executeInThread(Runnable runnable) {
        new Thread(runnable).start();
    }
}
