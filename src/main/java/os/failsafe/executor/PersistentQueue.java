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

import os.failsafe.executor.task.Task;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Timestamp;
import java.util.List;

class PersistentQueue {

    private static final int DEAD_EXECUTIONS_TIMEOUT_IN_MINUTES = 10;
    private static final String QUERY_ALL_TASKS = "SELECT * FROM PERSISTENT_TASK WHERE FAILED = 0 AND (LOCK_TIME IS NULL OR (LOCK_TIME <= ?)) ORDER BY CREATED_DATE";
    private static final String QUERY_LIMIT = " FETCH FIRST 3 ROWS ONLY";
    private static final String QUERY_LIMIT_MYSQL = " LIMIT 3";

    private final String QUERY_NEXT_TASKS;

    private final Database database;
    private final SystemClock systemClock;
    private final PersistentTasks persistentTasks;

    public PersistentQueue(Database database, SystemClock systemClock) {
        this.database = database;
        this.systemClock = systemClock;
        this.persistentTasks = new PersistentTasks(database, systemClock);

        this.QUERY_NEXT_TASKS = constructNextTaskQuery();
    }

    PersistentTask add(Task task) {
        return persistentTasks.create(task);
    }

    List<PersistentTask> allQueued() {
        return database.selectAll(QUERY_ALL_TASKS, persistentTasks::map, deadExecutionTimeout());
    }

    PersistentTask peekAndLock() {
        return database.connect(connection ->

                database.selectAll(connection, QUERY_NEXT_TASKS, persistentTasks::map, deadExecutionTimeout()).stream()
                        .map(enqueuedTask -> enqueuedTask.lock(connection))
                        .findFirst()
                        .orElse(null));
    }

    private Timestamp deadExecutionTimeout() {
        return Timestamp.valueOf(systemClock.now().minusMinutes(DEAD_EXECUTIONS_TIMEOUT_IN_MINUTES));
    }

    private String constructNextTaskQuery() {
        return QUERY_ALL_TASKS + (database.isMysql() ? QUERY_LIMIT_MYSQL : QUERY_LIMIT);
    }

}
