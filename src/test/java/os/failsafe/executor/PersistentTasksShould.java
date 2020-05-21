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
import os.failsafe.executor.task.FailedTask;
import os.failsafe.executor.task.Task;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistentTasksShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private PersistentTasks persistentTasks;
    private String taskName = "TestTask";
    private final String taskParameter = "parameter";

    @BeforeEach
    public void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        persistentTasks = new PersistentTasks(database, systemClock);
    }

    @Test
    public void
    persist_and_find_a_task() {
        PersistentTask task = createTask();

        PersistentTask actual = persistentTasks.findOne(task.getId());
        assertDoesNotThrow(() -> UUID.fromString(actual.getId().id));
        assertEquals(task.getId(), actual.getId());
        assertEquals(taskName, actual.getName());
        assertEquals(taskParameter, actual.getParameter());
        assertEquals(0L, actual.version);
        assertNull(actual.startTime);
    }

    @Test
    public void
    persist_a_task_with_given_id() {
        String id = "id";
        PersistentTask task = createTask(id);

        PersistentTask actual = persistentTasks.findOne(task.getId());
        assertEquals(id, task.getId().id);
        assertEquals(id, actual.getId().id);
    }

    @Test
    public void
    raise_and_exception_if_id_is_not_unique() {
        String id = "id";
        createTask(id);
        assertThrows(RuntimeException.class, () -> createTask(id));
    }

    @Test
    public void
    return_empty_list_if_no_task_is_failed() {
        createTask();

        List<FailedTask> failedTasks = persistentTasks.failedTasks();
        assertTrue(failedTasks.isEmpty());
    }

    @Test
    public void
    return_empty_optional_if_task_is_not_failed() {
        PersistentTask persistentTask = createTask();

        Optional<FailedTask> failedTasks = persistentTasks.failedTask(persistentTask.getId());
        assertFalse(failedTasks.isPresent());
    }

    private PersistentTask createTask() {
        return persistentTasks.create(new Task(taskName, taskParameter));
    }

    private PersistentTask createTask(String id) {
        return persistentTasks.create(new Task(id, taskName, taskParameter));
    }
}
