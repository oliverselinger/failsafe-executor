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
    private static final String FAIL_TASK = "UPDATE PERSISTENT_TASK SET FAILED=1, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=? WHERE ID=? AND VERSION=?";
    private static final String RETRY_TASK = "UPDATE PERSISTENT_TASK SET LOCK_TIME=null, FAILED=0, EXCEPTION_MESSAGE=null, STACK_TRACE=null, VERSION=? WHERE ID=? AND VERSION=?";

    private final String id;
    private final String parameter;
    private final String name;
    final LocalDateTime startTime;
    private final Long version;
    final boolean failed;
    final String exceptionMessage;
    final String stackTrace;
    private final Database database;
    private final SystemClock systemClock;

    public PersistentTask(String id, String parameter, String name, Database database, SystemClock systemClock) {
        this(id, parameter, name, null, 0L, false, null, null, database, systemClock);
    }

    public PersistentTask(String id, String parameter, String name, LocalDateTime startTime, Long version, boolean failed, String exceptionMessage, String stackTrace, Database database, SystemClock systemClock) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.startTime = startTime;
        this.version = version;
        this.failed = failed;
        this.exceptionMessage = exceptionMessage;
        this.stackTrace = stackTrace;
        this.database = database;
        this.systemClock = systemClock;
    }

    PersistentTask lock(Connection connection) {
        LocalDateTime startTime = systemClock.now();

        int effectedRows = database.update(connection, SET_LOCK_TIME,
                version + 1,
                Timestamp.valueOf(startTime),
                version,
                id);
        if (effectedRows == 1) {
            return new PersistentTask(id, parameter, name, startTime, version + 1, false, null, null, database, systemClock);
        }

        return null;
    }

    void remove() {
        database.delete(DELETE_TASK, id, version);
    }

    void fail(Exception exception) {
        String message = StringUtils.abbreviate(exception.getMessage(), 1000);
        String stackTrace = ExceptionUtils.stackTraceAsString(exception);

        int updateCount = database.update(FAIL_TASK, message, stackTrace, version+1, id, version);

        if (updateCount != 1) {
            throw new RuntimeException("Couldn't set task to failed.");
        }
    }

    public void retry() {
        int updateCount = database.update(RETRY_TASK, version+1, id, version);

        if (updateCount != 1) {
            throw new RuntimeException("Couldn't retry task.");
        }
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getParameter() {
        return parameter;
    }
}
