package os.failsafe.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPoolShould {

    private static final TestSystemClock systemClock = new TestSystemClock();

    private int threadCount = 2;
    private int queueSize = threadCount * 2;
    private WorkerPool workerPool;

    @BeforeEach
    void init() {
        workerPool = new WorkerPool(threadCount, queueSize);
    }

    @AfterEach
    void stop() {
        workerPool.stop(15, TimeUnit.SECONDS);
    }

    @Test
    void accept_more_tasks_if_workers_are_idle() {
        assertFalse(workerPool.allWorkersBusy());
    }

    @Test
    void not_accept_more_tasks_if_all_workers_are_busy() throws InterruptedException, ExecutionException {
        BlockingRunnable firstBlockingRunnable = new BlockingRunnable();
        Future<String> execution = workerPool.execute(UUID.randomUUID().toString(), firstBlockingRunnable);

        List<BlockingRunnable> blockingRunnables = IntStream.range(1, queueSize)
                .mapToObj(i -> new BlockingRunnable())
                .peek(blockingRunnable -> workerPool.execute(UUID.randomUUID().toString(), blockingRunnable))
                .collect(Collectors.toList());

        assertTrue(workerPool.allWorkersBusy());

        firstBlockingRunnable.release();
        execution.get();

        assertFalse(workerPool.allWorkersBusy());

        BlockingRunnable nextBlockingRunnable = new BlockingRunnable();
        workerPool.execute(UUID.randomUUID().toString(), nextBlockingRunnable);

        assertTrue(workerPool.allWorkersBusy());

        nextBlockingRunnable.release();
        blockingRunnables.forEach(BlockingRunnable::release);
    }

    static class BlockingRunnable implements Runnable {
        Phaser phaser;

        BlockingRunnable() {
            phaser = new Phaser(2);
        }

        @Override
        public void run() {
            phaser.arriveAndAwaitAdvance();
        }

        public void release() {
            phaser.arrive();
        }
    }
}
