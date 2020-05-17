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
import java.util.function.Consumer;
import java.util.function.Function;

public class Database {

    private Database() {
    }

    public static <T> T selectOne(DataSource dataSource,
                                  String sql,
                                  Function<ResultSet, T> mapper,
                                  Object... params) {

        try (Connection connection = dataSource.getConnection()) {
            List<T> selection = selectAll(connection, sql, mapper, params);

            if (selection.isEmpty()) {
                return null;
            }

            if (selection.size() > 1) {
                throw new RuntimeException("Too many results");
            }

            return selection.get(0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> selectAll(DataSource dataSource,
                                        String sql,
                                        Function<ResultSet, T> mapper,
                                        Object... params) {

        try (Connection connection = dataSource.getConnection()) {
            return selectAll(connection, sql, mapper, params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> List<T> selectAll(Connection connection,
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

    public static <T> void insert(DataSource dataSource,
                                  String sql,
                                  Object... params) {
        int effectedRows = executeUpdate(dataSource, sql, params);

        if (effectedRows != 1) {
            throw new RuntimeException("Insertion failure");
        }
    }

    public static <T> int update(Connection connection,
                                 String sql,
                                 Object... params) {
        return executeUpdate(connection, sql, params);
    }

    public static <T> int update(DataSource dataSource,
                                 String sql,
                                 Object... params) {
        return executeUpdate(dataSource, sql, params);
    }

    public static <T> int delete(DataSource dataSource,
                                 String sql,
                                 Object... params) {
        return executeUpdate(dataSource, sql, params);
    }

    public static <T> int executeUpdate(DataSource dataSource,
                                        String sql,
                                        Object... params) {
        try (Connection connection = dataSource.getConnection()) {
            return executeUpdate(connection, sql, params);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> int executeUpdate(Connection connection,
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

    public static void execute(Connection connection, String... sqlStatements) {
        try (Statement statement = connection.createStatement()) {
            for (String sqlStatement : sqlStatements) {
                statement.execute(sqlStatement);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T runAndReturn(DataSource dataSource, Function<Connection, T> connectionConsumer) {
        try (Connection connection = dataSource.getConnection()) {
            return connectionConsumer.apply(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void run(DataSource dataSource, Consumer<Connection> connectionConsumer) {
        try (Connection connection = dataSource.getConnection()) {
            connectionConsumer.accept(connection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
