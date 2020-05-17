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
import org.junit.jupiter.api.extension.RegisterExtension;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.task.Task;
import os.failsafe.executor.task.TaskDefinition;
import os.failsafe.executor.task.TaskDefinitions;
import os.failsafe.executor.utils.TestSystemClock;

import javax.sql.DataSource;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PersistentQueueShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private TestSystemClock systemClock = new TestSystemClock();

    private DataSource dataSource;
    private PersistentQueue persistentQueue;
    private PersistentTasks persistentTasks;

    @BeforeEach
    public void init() {
        dataSource = DB_EXTENSION.getDataSource();
        systemClock = new TestSystemClock();
        persistentTasks = new PersistentTasks(dataSource, systemClock);
        persistentQueue = new PersistentQueue(dataSource, systemClock);
    }

    @Test public void
    return_null_if_queue_is_empty() {
        assertNull(persistentQueue.peekAndLock());
    }

    @Test public void
    store_and_enqueue_a_task() {
        Task task = createTask();

        PersistentTask persistentTask = persistentQueue.add(task);

        List<PersistentTask> all = persistentQueue.allQueued();
        assertEquals(1, all.size());

        PersistentTask actual = all.get(0);
        assertEquals(task.name, actual.getName());
        assertEquals(task.parameter, actual.getParameter());
        assertEquals(persistentTask.getId(), actual.getId());
    }

    @Test public void
    dequeue_tasks_in_fifo_sequence() {
        Task task = createTask();
        PersistentTask persistentTask1 = persistentQueue.add(task);
        PersistentTask persistentTask2 = persistentQueue.add(task);
        PersistentTask persistentTask3 =  persistentQueue.add(task);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();
        PersistentTask dequedTask2 = persistentQueue.peekAndLock();
        PersistentTask dequedTask3 = persistentQueue.peekAndLock();

        assertEquals(persistentTask1.getId(), dequedTask1.getId());
        assertEquals(persistentTask2.getId(), dequedTask2.getId());
        assertEquals(persistentTask3.getId(), dequedTask3.getId());
    }

    @Test public void
    retrieve_a_task_again_after_lock_times_out() {
        Task task = createTask();
        persistentQueue.add(task);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();

        systemClock.timeTravelBy(Duration.ofMinutes(10));

        PersistentTask dequedTask2 = persistentQueue.peekAndLock();

        assertNotNull(dequedTask1);
        assertNotNull(dequedTask2);
        assertEquals(dequedTask1.getId(), dequedTask2.getId());
    }

    @Test public void
    never_retrieve_a_failed_task() {
        Task task = createTask();
        persistentQueue.add(task);

        PersistentTask dequedTask1 = persistentQueue.peekAndLock();

        dequedTask1.fail(new RuntimeException());

        systemClock.timeTravelBy(Duration.ofMinutes(10));

        PersistentTask dequedTask2 = persistentQueue.peekAndLock();

        assertNotNull(dequedTask1);
        assertNull(dequedTask2);
    }

    private Task createTask() {
        TaskDefinition taskDefinition = TaskDefinitions.of("TaskName", parameter -> {
        });
        return taskDefinition.newTask("Hello world!");
    }
}
