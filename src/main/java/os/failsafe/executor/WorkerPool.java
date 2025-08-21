package os.failsafe.executor;

import os.failsafe.executor.utils.Log;
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
    private final Log logger = Log.get(WorkerPool.class);

    WorkerPool(int threadCount, int queueSize, HeartbeatService heartbeatService) {
        this.threadCount = threadCount;
        spareQueueCount = new AtomicInteger(queueSize);
        this.heartbeatService = heartbeatService;
    }

    void start() {
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
        logger.info("Started worker pool with " + threadCount + " threads");
    }

    public Future<String> execute(Task task, Runnable runnable) {
        spareQueueCount.decrementAndGet();
        heartbeatService.register(task);
        logger.debug("Executing task: " + task.getName() + " (ID: " + task.getId() + ")");

        return workers.submit(() -> {
            try {
                logger.debug("Started task: " + task.getName() + " (ID: " + task.getId() + ")");
                runnable.run();
                logger.debug("Completed task: " + task.getName() + " (ID: " + task.getId() + ")");
            } catch (Exception e) {
                logger.error("Error executing task: " + task.getName() + " (ID: " + task.getId() + ")", e);
                throw e; // rethrow to mark the Future as failed
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
            logger.debug("Worker pool already stopped or never started");
            return;
        }

        logger.info("Shutting down worker pool");
        workers.shutdown();
        try {
            boolean terminated = workers.awaitTermination(timeout, timeUnit);
            if (terminated) {
                logger.info("Worker pool shutdown completed successfully");
            } else {
                logger.warn("Worker pool did not terminate within the timeout period of " + timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            logger.error("Worker pool shutdown was interrupted", e);
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }

}
