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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;

public class Incidents {

    private static final String INSERT_INCIDENT = "INSERT INTO TASK_INCIDENT (ID,MESSAGE,STACK_TRACE,TASK_ID,LAST_MODIFIED_DATE,CREATED_DATE) VALUES (?,?,?,?,?,?)";

    private final DataSource dataSource;
    private final SystemClock systemClock;

    public Incidents(DataSource dataSource, SystemClock systemClock) {
        this.dataSource = dataSource;
        this.systemClock = systemClock;
    }

    String create(Connection connection, String taskId, Exception e) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_INCIDENT)) {
            String id = UUID.randomUUID().toString();
            ps.setString(1, id);
            ps.setString(2, e.getMessage());
            ps.setString(3, e.getStackTrace().toString());
            ps.setString(4, taskId);
            ps.setTimestamp(5, Timestamp.valueOf(systemClock.now()));
            ps.setTimestamp(6, Timestamp.valueOf(systemClock.now()));

            int insertedRows = ps.executeUpdate();

            if (insertedRows != 1) {
                throw new RuntimeException("Insertion failure");
            }

            return id;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }
}
