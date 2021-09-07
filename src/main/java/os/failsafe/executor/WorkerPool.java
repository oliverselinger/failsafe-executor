package os.failsafe.executor;

import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerPool {

    private final AtomicInteger spareQueueCount;
    private final ExecutorService workers;

    WorkerPool(int threadCount, int queueSize) {
        spareQueueCount = new AtomicInteger(queueSize);
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
    }

    public Future<String> execute(String taskId, Runnable runnable) {
        spareQueueCount.decrementAndGet();
        return workers.submit(() -> {
            try {
                runnable.run();
            } finally {
                spareQueueCount.incrementAndGet();
            }
            return taskId;
        });
    }

    int spareQueueCount() {
        return spareQueueCount.get();
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
