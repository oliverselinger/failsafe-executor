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

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

class EnqueuedTasks {

    private static final String INSERT_TASK = "INSERT INTO PERSISTENT_TASK (ID,NAME,PARAMETER,VERSION,LAST_MODIFIED_DATE,CREATED_DATE) VALUES (?,?,?,?,?,?)";
    private static final String QUERY_ALL = "SELECT * FROM PERSISTENT_TASK";
    private static final String QUERY_ONE = QUERY_ALL + " WHERE ID=?";
    private static final String QUERY_ALL_FAILED = QUERY_ALL + " WHERE FAILED=1";

    private final DataSource dataSource;
    private final SystemClock systemClock;

    public EnqueuedTasks(DataSource dataSource, SystemClock systemClock) {
        this.dataSource = dataSource;
        this.systemClock = systemClock;
    }

    PersistentTask create(Task task) {
        String id = generateId();

        Database.insert(dataSource, INSERT_TASK,
                id,
                task.name,
                task.parameter,
                0,
                Timestamp.valueOf(systemClock.now()),
                Timestamp.valueOf(systemClock.now()));

        return new PersistentTask(id, task.parameter, task.name, dataSource, systemClock);
    }

    PersistentTask findOne(String id) {
        return Database.selectOne(dataSource, QUERY_ONE, this::map, id);
    }

    List<PersistentTask> failedTasks() {
        return Database.selectAll(dataSource, QUERY_ALL_FAILED, this::map);
    }

    PersistentTask map(ResultSet rs) {
        try {
            Timestamp pickTime = rs.getTimestamp("LOCK_TIME");

            return new PersistentTask(
                    rs.getString("ID"),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    pickTime!=null ? pickTime.toLocalDateTime() : null,
                    rs.getLong("VERSION"),
                    rs.getBoolean("FAILED"),
                    rs.getString("EXCEPTION_MESSAGE"),
                    rs.getString("STACK_TRACE"),
                    dataSource,
                    systemClock);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateId() {
        return UUID.randomUUID().toString();
    }

}
