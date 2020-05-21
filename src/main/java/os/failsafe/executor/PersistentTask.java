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
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.StringUtils;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.Timestamp;
import java.time.LocalDateTime;

class PersistentTask {

    private static final String SET_LOCK_TIME = "UPDATE PERSISTENT_TASK SET VERSION=?, LOCK_TIME=? WHERE VERSION=? AND ID=?";
    private static final String DELETE_TASK = "DELETE FROM PERSISTENT_TASK WHERE ID=? AND VERSION=?";
    private static final String FAIL_TASK = "UPDATE PERSISTENT_TASK SET FAILED=1, FAIL_TIME=?, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=? WHERE ID=? AND VERSION=?";

    private final TaskId id;
    private final String parameter;
    private final String name;
    final LocalDateTime startTime;
    final Long version;
    private final Database database;
    private final SystemClock systemClock;

    public PersistentTask(String id, String parameter, String name, Database database, SystemClock systemClock) {
        this(new TaskId(id), parameter, name, null, 0L, database, systemClock);
    }

    public PersistentTask(TaskId id, String parameter, String name, LocalDateTime startTime, Long version, Database database, SystemClock systemClock) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.startTime = startTime;
        this.version = version;
        this.database = database;
        this.systemClock = systemClock;
    }

    PersistentTask lock(Connection connection) {
        LocalDateTime startTime = systemClock.now();

        int effectedRows = database.update(connection, SET_LOCK_TIME,
                version + 1,
                Timestamp.valueOf(startTime),
                version,
                id.id);
        if (effectedRows == 1) {
            return new PersistentTask(id, parameter, name, startTime, version + 1, database, systemClock);
        }

        return null;
    }

    void remove() {
        database.delete(DELETE_TASK, id.id, version);
    }

    void fail(Exception exception) {
        String message = StringUtils.abbreviate(exception.getMessage(), 1000);
        String stackTrace = ExceptionUtils.stackTraceAsString(exception);

        int updateCount = database.update(FAIL_TASK, Timestamp.valueOf(systemClock.now()), message, stackTrace, version+1, id.id, version);

        if (updateCount != 1) {
            throw new RuntimeException("Couldn't set task to failed.");
        }
    }

    public TaskId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getParameter() {
        return parameter;
    }
}
