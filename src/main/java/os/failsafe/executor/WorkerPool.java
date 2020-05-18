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
