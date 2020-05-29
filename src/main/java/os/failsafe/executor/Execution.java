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

import os.failsafe.executor.task.FailsafeTask;
import os.failsafe.executor.task.Schedule;
import os.failsafe.executor.task.TaskExecutionListener;
import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.SystemClock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

class Execution {

    private final FailsafeTask failsafeTask;
    private final PersistentTask persistentTask;
    private final List<TaskExecutionListener> listeners;
    private final Schedule schedule;
    private final SystemClock systemClock;

    Execution(FailsafeTask failsafeTask, PersistentTask persistentTask, List<TaskExecutionListener> listeners, Schedule schedule, SystemClock systemClock) {
        this.failsafeTask = failsafeTask;
        this.persistentTask = persistentTask;
        this.listeners = listeners;
        this.schedule = schedule;
        this.systemClock = systemClock;
    }

    public TaskId perform() {
        try {
            failsafeTask.run(persistentTask.getParameter());

            notifySuccess();

            Optional<LocalDateTime> nextExecutionTime = schedule.nextExecutionTime(systemClock.now());
            if (nextExecutionTime.isPresent()) {
                persistentTask.nextExecution(nextExecutionTime.get());
            } else {
                persistentTask.remove();
            }
        } catch (Exception e) {
            persistentTask.fail(e);

            notifyFailed();
        }

        return persistentTask.getId();
    }

    private void notifySuccess() {
        listeners.forEach(this::notifySuccess);
    }

    private void notifySuccess(TaskExecutionListener listener) {
        listener.succeeded(persistentTask.getName(), persistentTask.getId(), persistentTask.getParameter());
    }

    private void notifyFailed() {
        listeners.forEach(this::notifyFailed);
    }

    private void notifyFailed(TaskExecutionListener listener) {
        listener.failed(persistentTask.getName(), persistentTask.getId(), persistentTask.getParameter());
    }
}
