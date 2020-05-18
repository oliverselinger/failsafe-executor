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
package os.failsafe.executor.utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Database {

    public final boolean mysqlDatabase;
    private final DataSource dataSource;

    public Database(DataSource dataSource) {
        this.dataSource = dataSource;
        this.mysqlDatabase = determineDatabase();
    }

    public boolean isMysql() {
        return mysqlDatabase;
    }

    public <T> T selectOne(String sql,
                           Function<ResultSet, T> mapper,
                           Object... params) {
        return connect(connection -> {
            List<T> selection = selectAll(connection, sql, mapper, params);

            if (selection.isEmpty()) {
                return null;
            }

            if (selection.size() > 1) {
                throw new RuntimeException("Too many results");
            }

            return selection.get(0);
        });
    }

    public <T> List<T> selectAll(String sql,
                                 Function<ResultSet, T> mapper,
                                 Object... params) {
        return connect(connection -> selectAll(connection, sql, mapper, params));
    }

    public <T> List<T> selectAll(Connection connection,
                                 String sql,
                                 Function<ResultSet, T> resultSetConsumer,
                                 Object... params) {

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int cnt = 0;
            for (Object param : params) {
                ps.setObject(++cnt, param);
            }

            try (ResultSet rs = ps.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(resultSetConsumer.apply(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void insert(Connection connection,
                           String sql,
                           Object... params) {
        int effectedRows = executeUpdate(connection, sql, params);

        if (effectedRows != 1) {
            throw new RuntimeException("Insertion failure");
        }
    }

    public <T> int update(Connection connection,
                          String sql,
                          Object... params) {
        return executeUpdate(connection, sql, params);
    }

    public <T> int update(String sql,
                          Object... params) {
        return executeUpdate(sql, params);
    }

    public <T> int delete(String sql,
                          Object... params) {
        return executeUpdate(sql, params);
    }

    public <T> int executeUpdate(String sql,
                                 Object... params) {
        return connect(connection -> executeUpdate(connection, sql, params));
    }

    public <T> int executeUpdate(Connection connection,
                                 String sql,
                                 Object... params) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {

            int cnt = 0;
            for (Object param : params) {
                ps.setObject(++cnt, param);
            }

            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(String... sqlStatements) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (String sqlStatement : sqlStatements) {
                statement.execute(sqlStatement);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T connect(Function<Connection, T> connectionConsumer) {
        try (Connection connection = dataSource.getConnection()) {
            return connectionConsumer.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean determineDatabase() {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getMetaData().getDatabaseProductName().equalsIgnoreCase("MySQL");
        } catch (SQLException e) {
            throw new RuntimeException("Unable to determine database product name", e);
        }
    }
}
