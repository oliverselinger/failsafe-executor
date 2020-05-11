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

import os.failsafe.executor.db.H2DbExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class FailsafeExecutorTest {

    private static final Logger log = LoggerFactory.getLogger(FailsafeExecutorTest.class);

    private final TestSystemClock systemClock = new TestSystemClock();

    @RegisterExtension
    static final H2DbExtension h2DbExtension = new H2DbExtension();

    DataSource dataSource;
    FailsafeExecutor failsafeExecutor;
    Task helloWorldTask;

    @BeforeEach
    public void init() {
        dataSource = h2DbExtension.getDataSource();
        systemClock.resetTime();

        failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, 5, 1, 1);

        helloWorldTask = Tasks.of("TestTask", parameter -> log.info("Hello {}", parameter));
        failsafeExecutor.register(helloWorldTask);
    }

    @AfterEach
    public void stop() {
        failsafeExecutor.stop();
    }

    @Test
    public void shouldExecuteNextTask() throws InterruptedException, TimeoutException, ExecutionException {
        Task.Instance instance = helloWorldTask.instance(" world!");

        String taskInstanceId = failsafeExecutor.execute(instance);

        Future<String> execution = this.failsafeExecutor.submitNextExecution().get();
        String actualTaskId = execution.get(5, TimeUnit.SECONDS);

        assertEquals(taskInstanceId, actualTaskId);
    }

    @Test
    public void shouldExecuteNextTaskViaScheduler() {
        Task.ExecutionEndedListener executionEndedListener = Mockito.mock(Task.ExecutionEndedListener.class);
        helloWorldTask.subscribe(executionEndedListener);

        Task.Instance instance = helloWorldTask.instance(" world!");

        String taskInstanceId = failsafeExecutor.execute(instance);

        this.failsafeExecutor.start();

        verify(executionEndedListener, timeout((int) TimeUnit.SECONDS.toMillis(5))).executed(helloWorldTask.getName(), taskInstanceId);
    }

    @Test
    public void shouldNotExecuteTaskThatIsAlreadyStarted() throws SQLException {
        Task.Instance instance = helloWorldTask.instance(" world!");

        String taskInstanceId = failsafeExecutor.execute(instance);

        TaskInstances taskInstances = new TaskInstances(dataSource, systemClock);

        TaskInstance task;
        try (Connection connection = dataSource.getConnection()) {
            task = taskInstances.findNextTask(connection).get();
            task.take(connection);
        }

        Optional<Future<String>> execution = failsafeExecutor.submitNextExecution();
        assertTrue(execution.isEmpty());

        task.delete();
    }

    @Test
    public void shouldExecuteTaskThatIsOlderThanTimeout() throws InterruptedException, TimeoutException, ExecutionException, SQLException {
        Task.Instance instance = helloWorldTask.instance(" world!");

        String taskInstanceId = failsafeExecutor.execute(instance);

        TaskInstances taskInstances = new TaskInstances(dataSource, systemClock);

        TaskInstance task;
        try (Connection connection = dataSource.getConnection()) {
            task = taskInstances.findNextTask(connection).get();
            task.take(connection);
        }

        systemClock.timeTravelBy(Duration.ofMinutes(10));

        Future<String> execution = failsafeExecutor.submitNextExecution().get();
        String actualTaskId = execution.get(5, TimeUnit.SECONDS);

        assertEquals(task.id, actualTaskId);
    }

}