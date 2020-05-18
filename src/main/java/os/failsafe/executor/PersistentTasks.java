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

import os.failsafe.executor.task.FailedTask;
import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.task.Task;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class PersistentTasks {

    private static final String INSERT_TASK = "INSERT INTO PERSISTENT_TASK (ID,NAME,PARAMETER,VERSION,LAST_MODIFIED_DATE,CREATED_DATE) VALUES (?,?,?,?,?,?)";
    private static final String QUERY_ALL = "SELECT * FROM PERSISTENT_TASK";
    private static final String QUERY_ONE = QUERY_ALL + " WHERE ID=?";
    private static final String QUERY_ALL_FAILED = QUERY_ALL + " WHERE FAILED=1";
    private static final String QUERY_ONE_FAILED = QUERY_ONE + " AND FAILED=1";

    private final Database database;
    private final SystemClock systemClock;

    public PersistentTasks(Database database, SystemClock systemClock) {
        this.database = database;
        this.systemClock = systemClock;
    }

    PersistentTask create(Task task) {
        return database.connect(connection -> this.create(connection, task));
    }

    PersistentTask create(Connection connection, Task task) {
        database.insert(connection, INSERT_TASK,
                task.id,
                task.name,
                task.parameter,
                0,
                Timestamp.valueOf(systemClock.now()),
                Timestamp.valueOf(systemClock.now()));

        return new PersistentTask(task.id, task.parameter, task.name, database, systemClock);
    }

    PersistentTask findOne(TaskId id) {
        return database.selectOne(QUERY_ONE, this::mapToPersistentTask, id.id);
    }

    List<FailedTask> failedTasks() {
        return database.selectAll(QUERY_ALL_FAILED, this::mapToFailedTask);
    }

    Optional<FailedTask> failedTask(TaskId taskId) {
        return Optional.ofNullable(database.selectOne(QUERY_ONE_FAILED, this::mapToFailedTask, taskId.id));
    }

    PersistentTask mapToPersistentTask(ResultSet rs) {
        try {
            Timestamp pickTime = rs.getTimestamp("LOCK_TIME");

            return new PersistentTask(
                    new TaskId(rs.getString("ID")),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    pickTime != null ? pickTime.toLocalDateTime() : null,
                    rs.getLong("VERSION"),
                    rs.getBoolean("FAILED"),
                    rs.getString("EXCEPTION_MESSAGE"),
                    rs.getString("STACK_TRACE"),
                    database,
                    systemClock);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    FailedTask mapToFailedTask(ResultSet rs) {
        try {
            Timestamp pickTime = rs.getTimestamp("LOCK_TIME");

            return new FailedTask(
                    new TaskId(rs.getString("ID")),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    pickTime != null ? pickTime.toLocalDateTime() : null,
                    rs.getLong("VERSION"),
                    rs.getString("EXCEPTION_MESSAGE"),
                    rs.getString("STACK_TRACE"),
                    database);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
