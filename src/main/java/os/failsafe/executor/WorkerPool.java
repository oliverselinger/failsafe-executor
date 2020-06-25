package os.failsafe.executor;

import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerPool {

    private final AtomicInteger idleWorkerCount;
    private final ExecutorService workers;

    WorkerPool(int threadCount, int queueSize) {
        idleWorkerCount = new AtomicInteger(queueSize);
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
    }

    public Future<String> execute(String taskId, Runnable runnable) {
        idleWorkerCount.decrementAndGet();
        return workers.submit(() -> {
            try {
                runnable.run();
            } finally {
                idleWorkerCount.incrementAndGet();
            }
            return taskId;
        });
    }

    boolean allWorkersBusy() {
        return !(idleWorkerCount.get() > 0);
    }

    void stop(long timeout, TimeUnit timeUnit) {
        workers.shutdown();
        try {
            workers.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // ignore
        }
    }

}
