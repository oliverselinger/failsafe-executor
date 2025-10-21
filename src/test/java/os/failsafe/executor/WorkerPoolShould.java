package os.failsafe.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.BlockingRunnable;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class WorkerPoolShould {

    private static final TestSystemClock systemClock = new TestSystemClock();

    private int threadCount = 2;
    private int queueSize = 2;
    private WorkerPool workerPool;
    private HeartbeatService heartbeatService;

    @BeforeEach
    void init() {
        heartbeatService = Mockito.mock(HeartbeatService.class);
        workerPool = new WorkerPool(threadCount, queueSize, heartbeatService);
        workerPool.start();
    }

    @AfterEach
    void stop() {
        workerPool.stop(15, TimeUnit.SECONDS);
    }

    @Test
    void accept_more_tasks_if_workers_are_idle() {
        assertEquals(0, workerPool.getQueuedTaskCount());
    }

    @Test
    void not_accept_more_tasks_if_all_workers_are_busy() throws InterruptedException, ExecutionException {
        BlockingRunnable firstBlockingRunnable = new BlockingRunnable();
        Future<String> execution = workerPool.execute(createTask(), firstBlockingRunnable);

        firstBlockingRunnable.waitForSetup();

        List<BlockingRunnable> blockingRunnables = IntStream.range(1, queueSize + threadCount)
                .mapToObj(i -> new BlockingRunnable())
                .peek(blockingRunnable -> workerPool.execute(createTask(), blockingRunnable))
                .collect(Collectors.toList());

        assertEquals(0, workerPool.freeSlots());

        firstBlockingRunnable.release();
        execution.get();

        Thread.sleep(10);

        int actual = workerPool.freeSlots();
        System.out.println(actual);
        assertEquals(1, actual);

        BlockingRunnable nextBlockingRunnable = new BlockingRunnable();
        workerPool.execute(createTask(), nextBlockingRunnable);

        assertEquals(0, workerPool.freeSlots());

        blockingRunnables.forEach(BlockingRunnable::waitForSetupAndRelease);
        nextBlockingRunnable.waitForSetupAndRelease();
    }

    @Test
    void register_task_for_heartbeating() throws InterruptedException, ExecutionException {
        BlockingRunnable firstBlockingRunnable = new BlockingRunnable();
        Future<String> execution = workerPool.execute(createTask(), firstBlockingRunnable);

        verifyRegister(1);
        verifyUnregister(0);

        firstBlockingRunnable.waitForSetupAndRelease();
        execution.get();

        verifyRegister(1);
        verifyUnregister(1);
    }

    private void verifyRegister(int times) {
        verify(heartbeatService, times(times)).register(any());
    }

    private void verifyUnregister(int times) {
        verify(heartbeatService, times(times)).unregister(any());
    }

    private Task createTask() {
        return new Task(UUID.randomUUID().toString(), "Test", "param", systemClock.now());
    }
}
