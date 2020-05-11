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

import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

class TaskInstances {

    private static final int DEAD_EXECUTIONS_TIMEOUT_IN_MINUTES = 10;
    private static final String QUERY_NEXT_TASK = "SELECT ID,NAME,PARAMETER,START_TIME,FAILED,VERSION,LAST_MODIFIED_DATE,CREATED_DATE FROM TASK_INSTANCE WHERE START_TIME IS NULL OR (START_TIME <= ?) ORDER BY CREATED_DATE FETCH FIRST 1 ROWS ONLY";
    private static final String INSERT_TASK = "INSERT INTO TASK_INSTANCE (ID,NAME,PARAMETER,VERSION,LAST_MODIFIED_DATE,CREATED_DATE) VALUES (?,?,?,?,?,?)";

    private final DataSource dataSource;
    private final SystemClock systemClock;

    public TaskInstances(DataSource dataSource, SystemClock systemClock) {
        this.dataSource = dataSource;
        this.systemClock = systemClock;
    }

    String create(String name, String parameter) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(INSERT_TASK)) {

            String id = UUID.randomUUID().toString();
            ps.setString(1, id);
            ps.setString(2, name);
            ps.setString(3, parameter);
            ps.setLong(4, 0);
            ps.setTimestamp(5, Timestamp.valueOf(systemClock.now()));
            ps.setTimestamp(6, Timestamp.valueOf(systemClock.now()));

            int insertedRows = ps.executeUpdate();

            if (insertedRows != 1) {
                throw new RuntimeException("Insertion failure");
            }

            return id;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    Optional<TaskInstance> findNextTask(Connection connection) {
        try (PreparedStatement ps = connection.prepareStatement(QUERY_NEXT_TASK)) {
            ps.setTimestamp(1, Timestamp.valueOf(systemClock.now().minusMinutes(DEAD_EXECUTIONS_TIMEOUT_IN_MINUTES)));

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                } else {
                    Timestamp start_time = rs.getTimestamp("START_TIME");
                    return Optional.of(new TaskInstance(
                            rs.getString("ID"),
                            rs.getString("PARAMETER"),
                            rs.getString("NAME"),
                            rs.getBoolean("FAILED"),
                            start_time != null ? start_time.toLocalDateTime() : null,
                            rs.getLong("VERSION"),
                            rs.getTimestamp("LAST_MODIFIED_DATE").toInstant(),
                            rs.getTimestamp("CREATED_DATE").toInstant(),
                            dataSource,
                            systemClock));
//                    do {
//
//                    } while (rs.next());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
