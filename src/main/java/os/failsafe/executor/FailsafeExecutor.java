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
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.DefaultSystemClock;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailsafeExecutor {

    public static final int DEFAULT_WORKER_THREAD_COUNT = 5;
    public static final int DEFAULT_QUEUE_SIZE = DEFAULT_WORKER_THREAD_COUNT * 2;
    public static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(10);
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5);

    private final Map<String, TaskDefinition> tasksByIdentifier = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));
    private final PersistentQueue persistentQueue;
    private final WorkerPool workerPool;
    private final PersistentTasks persistentTasks;
    private final Duration initialDelay;
    private final Duration pollingInterval;

    public FailsafeExecutor(DataSource dataSource) {
        this(new DefaultSystemClock(), dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, DEFAULT_INITIAL_DELAY, DEFAULT_POLLING_INTERVAL);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval) {
        Database database = new Database(dataSource);
        this.persistentQueue = new PersistentQueue(database, systemClock);
        this.workerPool = new WorkerPool(workerThreadCount, queueSize);
        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
        this.persistentTasks = new PersistentTasks(database, systemClock);
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            while (executeNextTask() != null) {
            }
        }, initialDelay.toMillis(), pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
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
        return persistentTasks.failedTasks();
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
