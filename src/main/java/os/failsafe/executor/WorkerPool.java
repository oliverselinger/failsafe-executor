package os.failsafe.executor;

import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class WorkerPool {

    private final AtomicInteger idleWorkerCount;
    private final ExecutorService workers;

    WorkerPool(int threadCount, int queueSize) {
        idleWorkerCount = new AtomicInteger(queueSize);
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
    }

    public Future<TaskId> execute(Execution execution) {
        idleWorkerCount.decrementAndGet();
        return workers.submit(() -> runTask(execution));
    }

    private TaskId runTask(Execution execution) {
        try {
            return execution.perform();
        } finally {
            idleWorkerCount.incrementAndGet();
        }
    }

    boolean allWorkersBusy() {
        return !(idleWorkerCount.get() > 0);
    }

    public void stop() {
        this.workers.shutdown();
    }

}
