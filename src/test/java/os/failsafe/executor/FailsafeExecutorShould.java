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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.db.H2DbExtension;
import os.failsafe.executor.task.Task;
import os.failsafe.executor.task.TaskDefinition;
import os.failsafe.executor.task.TaskDefinitions;
import os.failsafe.executor.task.TaskExecutionListener;
import os.failsafe.executor.utils.TestSystemClock;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_QUEUE_SIZE;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_WORKER_THREAD_COUNT;

public class FailsafeExecutorShould {

    private static final Logger log = LoggerFactory.getLogger(FailsafeExecutorShould.class);

    private final TestSystemClock systemClock = new TestSystemClock();

    @RegisterExtension
    static final H2DbExtension h2DbExtension = new H2DbExtension();

    DataSource dataSource;
    FailsafeExecutor failsafeExecutor;
    TaskDefinition taskDefinition;
    TaskExecutionListener taskExecutionListener;

    boolean executionShouldFail;

    @BeforeEach
    public void init() {
        dataSource = h2DbExtension.getDataSource();
        systemClock.resetTime();

        taskDefinition = TaskDefinitions.of("TestTask", parameter -> {
                    if (executionShouldFail) {
                        throw new RuntimeException();
                    }

                    log.info("Hello {}", parameter);
                }
        );

        failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1));
        failsafeExecutor.defineTask(taskDefinition);

        taskExecutionListener = Mockito.mock(TaskExecutionListener.class);
        taskDefinition.subscribe(taskExecutionListener);
    }

    @AfterEach
    public void stop() {
        failsafeExecutor.stop();
    }

    @Test()
    public void
    throw_an_exception_if_task_is_not_defined() {
        Task undefinedTask = new Task("TaskName", "parameter");

        assertThrows(IllegalArgumentException.class, () -> failsafeExecutor.execute(undefinedTask));
    }

    @Test
    public void
    execute_a_task() {
        Task task = taskDefinition.newTask(" world!");

        PersistentTask persistentTask = failsafeExecutor.execute(task);
        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).succeeded(taskDefinition.getName(), persistentTask.getId());
    }

    @Test
    public void
    retry_a_failed_task_on_demand() {
        Task task = taskDefinition.newTask(" world!");
        executionShouldFail = true;

        PersistentTask persistentTask = failsafeExecutor.execute(task);
        failsafeExecutor.start();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).failed(taskDefinition.getName(), persistentTask.getId());

        List<PersistentTask> failedTasks = failsafeExecutor.failedTasks();
        assertEquals(1, failedTasks.size());

        PersistentTask failedTask = failedTasks.get(0);

        executionShouldFail = false;
        failedTask.retry();

        verify(taskExecutionListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).succeeded(taskDefinition.getName(), persistentTask.getId());
    }
}