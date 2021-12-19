package os.failsafe.executor;

import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerPool {

    private final int threadCount;
    private final AtomicInteger spareQueueCount;
    private ExecutorService workers;
    private final HeartbeatService heartbeatService;

    WorkerPool(int threadCount, int queueSize, HeartbeatService heartbeatService) {
        this.threadCount = threadCount;
        spareQueueCount = new AtomicInteger(queueSize);
        this.heartbeatService = heartbeatService;
    }

    void start() {
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
    }

    public Future<String> execute(Task task, Runnable runnable) {
        spareQueueCount.decrementAndGet();
        heartbeatService.register(task);

        return workers.submit(() -> {
            try {
                runnable.run();
            } finally {
                heartbeatService.unregister(task);
                spareQueueCount.incrementAndGet();
            }
            return task.getId();
        });
    }

    int spareQueueCount() {
        return spareQueueCount.get();
    }

    void stop(long timeout, TimeUnit timeUnit) {
        if (workers == null) {
            return;
        }

        workers.shutdown();
        try {
            workers.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // ignore
        }
    }

}
