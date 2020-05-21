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
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.TestSystemClock;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PersistentTaskShould {

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    private final TestSystemClock systemClock = new TestSystemClock();
    private Database database;
    private PersistentTasks persistentTasks;

    @BeforeEach
    public void init() {
        database = DB_EXTENSION.database();
        systemClock.resetTime();
        persistentTasks = new PersistentTasks(database, systemClock);
    }

    @Test public void
    lock_itself() {
        PersistentTask persistentTask = createTask();

        database.connect(persistentTask::lock);

        PersistentTask locked = persistentTasks.findOne(persistentTask.getId());
        assertNotNull(locked.startTime);
    }

    @Test public void
    remove_itself() {
        PersistentTask persistentTask = createTask();

        persistentTask.remove();

        assertNull(persistentTasks.findOne(persistentTask.getId()));
    }

    @Test public void
    save_exception_details_and_mark_itself_as_failed() {
        PersistentTask persistentTask = createTask();

        String exceptionMessage = "Exception message";
        Exception exception = new Exception(exceptionMessage);

        persistentTask.fail(exception);

        List<FailedTask> failedTasks = persistentTasks.failedTasks();
        assertEquals(1, failedTasks.size());

        FailedTask failedTask = failedTasks.get(0);
        assertNotNull(failedTask.getFailTime());
        assertEquals(exceptionMessage, failedTask.getExceptionMessage());
        assertEquals(ExceptionUtils.stackTraceAsString(exception), failedTask.getStackTrace());
    }

    private PersistentTask createTask() {
        return persistentTasks.create(new Task("TestTask", "parameter"));
    }
}
