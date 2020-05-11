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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

class TaskInstance {

    private static final Logger log = LoggerFactory.getLogger(TaskInstance.class);

    private static final String TAKE_TASK = "UPDATE TASK_INSTANCE SET VERSION=?, START_TIME=? WHERE VERSION=? AND ID=?";
    private static final String DELETE_TASK = "DELETE FROM TASK_INSTANCE WHERE ID = ?";
    private static final String FAIL_TASK = "UPDATE TASK_INSTANCE SET FAILED = 1 WHERE ID = ?";

    final String id;
    final String parameter;
    final String name;
    private final boolean failed;
    private final LocalDateTime startTime;
    private final Long version;
    private final Instant lastModifiedDate;
    private final Instant createdDate;

    private final DataSource dataSource;
    private final SystemClock systemClock;

    private final Incidents incidents;

    public TaskInstance(String id, String parameter, String name, boolean failed, LocalDateTime startTime, Long version, Instant lastModifiedDate, Instant createdDate, DataSource dataSource, SystemClock systemClock) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.failed = failed;
        this.startTime = startTime;
        this.version = version;
        this.lastModifiedDate = lastModifiedDate;
        this.createdDate = createdDate;
        this.dataSource = dataSource;
        this.systemClock = systemClock;

        this.incidents = new Incidents(dataSource, systemClock);
    }

    boolean take(Connection connection) {
        try (PreparedStatement ps = connection.prepareStatement(TAKE_TASK)) {

            ps.setLong(1, version + 1);
            ps.setTimestamp(2, Timestamp.valueOf(systemClock.now()));
            ps.setLong(3, version);
            ps.setString(4, id);

            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException(e);

        }
    }

    void delete() {
        log.info("delete: " + id);

        try (Connection con = dataSource.getConnection();
             PreparedStatement ps = con.prepareStatement(DELETE_TASK)) {
            ps.setString(1, this.id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void fail(Exception exception) {
        log.error("Execution of task instance with id {} failed", id, exception);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (PreparedStatement ps = connection.prepareStatement(FAIL_TASK)) {
                ps.setString(1, this.id);
                ps.executeUpdate();

                this.incidents.create(connection, id, exception);
            } catch (Exception e) {
                connection.rollback();
            }

            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
