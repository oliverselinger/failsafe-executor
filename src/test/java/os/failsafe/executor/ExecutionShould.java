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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.task.TaskExecutionListener;
import os.failsafe.executor.task.TaskDefinition;
import os.failsafe.executor.task.TaskId;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutionShould {

    private Execution execution;
    private TaskDefinition taskDefinition;

    private PersistentTask persistentTask;
    private final TaskId taskId = new TaskId("123");
    private final String parameter = "Hello world!";
    private final String taskName = "TestTask";

    @BeforeEach
    public void init() {
        taskDefinition = Mockito.mock(TaskDefinition.class);

        persistentTask = Mockito.mock(PersistentTask.class);
        when(persistentTask.getId()).thenReturn(taskId);
        when(persistentTask.getParameter()).thenReturn(parameter);
        when(persistentTask.getName()).thenReturn(taskName);

        execution = new Execution(taskDefinition, persistentTask);
    }

    @Test public void
    execute_task_with_parameter() {
        execution.perform();

        verify(taskDefinition).execute(parameter);
    }

    @Test public void
    notify_listeners_after_successful_execution() {
        TaskExecutionListener listener = Mockito.mock(TaskExecutionListener.class);
        when(taskDefinition.allListeners()).thenReturn(Collections.singletonList(listener));

        execution.perform();

        verify(listener).succeeded(taskName, taskId, parameter);
    }

    @Test public void
    remove_task_after_successful_execution() {
        execution.perform();

        verify(persistentTask).remove();
    }

    @Test public void
    notify_listeners_after_failed_execution() {
        TaskExecutionListener listener = Mockito.mock(TaskExecutionListener.class);
        when(taskDefinition.allListeners()).thenReturn(Collections.singletonList(listener));

        RuntimeException exception = new RuntimeException();
        doThrow(exception).when(taskDefinition).execute(any());

        execution.perform();

        verify(listener).failed(taskName, taskId, parameter);
    }

}
