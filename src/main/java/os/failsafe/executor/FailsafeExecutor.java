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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailsafeExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskInstance.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));
    private final Workers workers;
    private final DataSource dataSource;
    private final int initialDelay;
    private final int delay;
    private final TaskInstances taskInstances;

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int threadCount, int initialDelay, int delay) {
        this.workers = new Workers(threadCount);
        this.dataSource = dataSource;
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.taskInstances = new TaskInstances(dataSource, systemClock);
    }

    public void register(Task task) {
        this.workers.register(task);
    }

    public void start() {
        executor.scheduleWithFixedDelay(() -> {
            while (submitNextExecution().isPresent()) {
            }
        }, initialDelay, delay, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        this.workers.stop();
        executor.shutdown();
    }

    public String execute(Task.Instance instance) {
        return taskInstances.create(instance.name, instance.parameter);
    }

    Optional<Future<String>> submitNextExecution() {
        if (workers.allWorkersBusy()) {
            return Optional.empty();
        }

        try (Connection connection = dataSource.getConnection()) {

            Optional<TaskInstance> possibleTask = taskInstances.findNextTask(connection);

            if (possibleTask.isEmpty()) {
                return Optional.empty();
            }

            TaskInstance taskInstance = possibleTask.get();
            log.info("findNextTask: " + taskInstance.id);
            boolean taken = taskInstance.take(connection);

            if (!taken) {
                return submitNextExecution();
            }

            return Optional.of(workers.execute(taskInstance));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
