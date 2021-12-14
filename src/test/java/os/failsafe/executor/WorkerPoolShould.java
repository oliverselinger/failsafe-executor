package os.failsafe.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPoolShould {

    private static final TestSystemClock systemClock = new TestSystemClock();

    private int threadCount = 2;
    private int queueSize = threadCount * 2;
    private WorkerPool workerPool;

    @BeforeEach
    void init() {
        HeartbeatService heartbeatService = Mockito.mock(HeartbeatService.class);
        workerPool = new WorkerPool(threadCount, queueSize, heartbeatService);
        workerPool.start();
    }

    @AfterEach
    void stop() {
        workerPool.stop(15, TimeUnit.SECONDS);
    }

    @Test
    void accept_more_tasks_if_workers_are_idle() {
        assertEquals(queueSize, workerPool.spareQueueCount());
    }

    @Test
    void not_accept_more_tasks_if_all_workers_are_busy() throws InterruptedException, ExecutionException {
        BlockingRunnable firstBlockingRunnable = new BlockingRunnable();
        Future<String> execution = workerPool.execute(createTask(), firstBlockingRunnable);

        List<BlockingRunnable> blockingRunnables = IntStream.range(1, queueSize)
                .mapToObj(i -> new BlockingRunnable())
                .peek(blockingRunnable -> workerPool.execute(createTask(), blockingRunnable))
                .collect(Collectors.toList());

        assertEquals(0, workerPool.spareQueueCount());

        firstBlockingRunnable.release();
        execution.get();

        assertEquals(1, workerPool.spareQueueCount());

        BlockingRunnable nextBlockingRunnable = new BlockingRunnable();
        workerPool.execute(createTask(), nextBlockingRunnable);

        assertEquals(0, workerPool.spareQueueCount());

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

    private Task createTask() {
        return new Task(UUID.randomUUID().toString(), "Test", "param", systemClock.now());
    }
}
