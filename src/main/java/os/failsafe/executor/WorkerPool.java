package os.failsafe.executor;

import os.failsafe.executor.utils.Log;
import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class WorkerPool {

    private final int threadCount;
    private ThreadPoolExecutor workers;
    private final int queueSize;
    private final HeartbeatService heartbeatService;
    private final Log logger = Log.get(WorkerPool.class);

    WorkerPool(int threadCount, int queueSize, HeartbeatService heartbeatService) {
        this.threadCount = threadCount;
        this.queueSize = queueSize;
        this.heartbeatService = heartbeatService;
    }

    void start() {
        workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
        logger.info("Started worker pool with " + threadCount + " threads");
    }

    public Future<String> execute(Task task, Runnable runnable) {
        heartbeatService.register(task);
        logger.debug("Submitting task: " + task.getName() + " (ID: " + task.getId() + ")");

        return workers.submit(() -> {
            try {
                logger.info("Starting task: " + task.getName() + " (ID: " + task.getId() + ")");

                runnable.run();

                logger.debug("Completed task: " + task.getName() + " (ID: " + task.getId() + ")");
            } catch (Exception e) {
                logger.error("Error executing task: " + task.getName() + " (ID: " + task.getId() + ")", e);
                throw e; // rethrow to mark the Future as failed
            } finally {
                heartbeatService.unregister(task);
            }
            return task.getId();
        });
    }

    int getQueuedTaskCount() {
        return workers.getQueue().size();
    }

    int freeSlots() {
        return queueSize - getQueuedTaskCount();
    }

    void stop(long timeout, TimeUnit timeUnit) {
        if (workers == null) {
            logger.debug("Worker pool already stopped or never started");
            return;
        }

        logger.info("Stopping worker pool with timeout: " + timeout + " " + timeUnit);

        workers.shutdown();
        try {
            boolean terminated = workers.awaitTermination(timeout, timeUnit);
            if (!terminated) {
                logger.warn("Worker pool did not terminate within the timeout period of " + timeout + " " + timeUnit);
            }
        } catch (InterruptedException e) {
            logger.error("Worker pool shutdown was interrupted", e);
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }
    }

}
