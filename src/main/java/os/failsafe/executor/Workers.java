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

import os.failsafe.executor.utils.NamedThreadFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

class Workers {

    private static final int FILL_UP_QUEUE_THRESHOLD = 3;

    private final Map<String, Task> tasksByIdentifier = new ConcurrentHashMap<>();
    private final AtomicInteger idleWorkerCount;
    private final ExecutorService workers;

    Workers(int threadCount) {
        idleWorkerCount = new AtomicInteger(threadCount + FILL_UP_QUEUE_THRESHOLD);
        workers = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("Failsafe-Worker-"));
    }

    public void register(Task task) {
        tasksByIdentifier.put(task.getName(), task);
    }

    public Future<String> execute(TaskInstance taskInstance) {
        idleWorkerCount.decrementAndGet();
        return workers.submit(() -> runTask(taskInstance));
    }

    private String runTask(TaskInstance taskInstance) {
        try {
            Task task = tasksByIdentifier.get(taskInstance.name);

            task.execute(taskInstance.parameter);

            task.notifyListeners(taskInstance.id);

            taskInstance.delete();
        } catch (Exception e) {
            taskInstance.fail(e);
        } finally {
            idleWorkerCount.incrementAndGet();
        }

        return taskInstance.id;
    }

    boolean allWorkersBusy() {
        return !(idleWorkerCount.get() > 0);
    }

    public void stop() {
        this.workers.shutdown();
    }
}
