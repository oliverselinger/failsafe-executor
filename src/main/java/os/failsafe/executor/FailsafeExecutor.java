/*******************************************************************************
 * MIT License
 *
 * Copyright (c) 2020 Oliver Selinger
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package os.failsafe.executor;

import os.failsafe.executor.task.Task;
import os.failsafe.executor.task.TaskDefinition;
import os.failsafe.executor.utils.DefaultSystemClock;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailsafeExecutor {

    private static final int DEFAULT_WORKER_THREAD_COUNT = 5;
    private static final int DEFAULT_INITIAL_DELAY_IN_SECONDS = 10;
    private static final int DEFAULT_POLLING_INTERVAL_IN_SECONDS = 10;

    private final Map<String, TaskDefinition> tasksByIdentifier = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));
    private final PersistentQueue persistentQueue;
    private final WorkerPool workerPool;
    private final EnqueuedTasks enqueuedTasks;
    private final int initialDelayInSeconds;
    private final int pollingIntervalInSeconds;

    public FailsafeExecutor(DataSource dataSource) {
        this(new DefaultSystemClock(), dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_INITIAL_DELAY_IN_SECONDS, DEFAULT_POLLING_INTERVAL_IN_SECONDS);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int initialDelayInSeconds, int pollingIntervalInSeconds) {
        this.persistentQueue = new PersistentQueue(dataSource, systemClock);
        this.workerPool = new WorkerPool(workerThreadCount);
        this.initialDelayInSeconds = initialDelayInSeconds;
        this.pollingIntervalInSeconds = pollingIntervalInSeconds;
        this.enqueuedTasks = new EnqueuedTasks(dataSource, systemClock);
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            while (executeNextTask() != null) {
            }
        }, initialDelayInSeconds, pollingIntervalInSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        this.workerPool.stop();
        executor.shutdown();
    }

    public void defineTask(TaskDefinition taskDefinition) {
        tasksByIdentifier.put(taskDefinition.getName(), taskDefinition);
    }

    public PersistentTask execute(Task task) {
        if (!tasksByIdentifier.containsKey(task.name)) {
            throw new IllegalArgumentException(String.format("Before executing task %s you need to define it. FailsafeExecutor#define", task.name));
        }
        return persistentQueue.add(task);
    }

    public List<PersistentTask> failedTasks() {
        return enqueuedTasks.failedTasks();
    }

    private Future<String> executeNextTask() {
        if (workerPool.allWorkersBusy()) {
            return null;
        }

        PersistentTask toExecute = persistentQueue.peekAndLock();

        if (toExecute == null) {
            return null;
        }

        TaskDefinition taskDefinition = tasksByIdentifier.get(toExecute.getName());

        return workerPool.execute(new Execution(taskDefinition, toExecute));
    }
}
