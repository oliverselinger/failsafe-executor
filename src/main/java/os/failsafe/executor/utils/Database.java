package os.failsafe.executor.utils;

import javax.sql.DataSource;
import java.io.StringReader;
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

    private final boolean oracleDatabase;
    private final boolean mysqlDatabase;
    private final boolean mariaDatabase;
    private final boolean postgresDatabase;
    private final boolean h2Database;
    private final DataSource dataSource;

    public Database(DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;

        String databaseName = determineDatabase();
        oracleDatabase = databaseName.equalsIgnoreCase("Oracle");
        mysqlDatabase = databaseName.equalsIgnoreCase("MySQL");
        postgresDatabase = databaseName.equalsIgnoreCase("PostgreSQL");
        h2Database = databaseName.equalsIgnoreCase("H2");
        mariaDatabase = databaseName.equalsIgnoreCase("MariaDB");

        if (!oracleDatabase && !mysqlDatabase && !postgresDatabase && !h2Database && !mariaDatabase) {
            throw new RuntimeException("Unsupported database");
        }
    }

    public boolean isOracle() {
        return oracleDatabase;
    }

    public boolean isMysqlOrMariaDb() {
        return mysqlDatabase || mariaDatabase;
    }

    public boolean isPostgres() {
        return postgresDatabase;
    }

    public boolean isH2() {
        return h2Database;
    }

    public <T> T selectOne(String sql,
                           RowMapper<T> rowMapper,
                           Object... params) {
        return connect(connection -> {
            List<T> selection = selectAll(connection, sql, rowMapper, params);

            if (selection.isEmpty()) {
                return null;
            }

            if (selection.size() > 1) {
                throw new RuntimeException("Too many results");
            }

            return selection.get(0);
        });
    }

    public <T> List<T> selectAll(String sql, RowMapper<T> rowMapper, Object... params) {
        return connect(connection -> selectAll(connection, sql, rowMapper, params));
    }

    public <T> List<T> selectAll(Connection connection, String sql, RowMapper<T> rowMapper, Object... params) {

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int cnt = 0;

            if (params != null) {
                for (Object param : params) {
                    ps.setObject(++cnt, param);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rowMapper.map(rs));
                }
                return result;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int insert(Connection connection,
                       String sql,
                       Object... params) {
        return executeUpdate(connection, sql, params);
    }

    public int update(String sql,
                      Object... params) {
        return executeUpdate(sql, params);
    }

    public int executeUpdate(String sql,
                             Object... params) {
        return connect(connection -> executeUpdate(connection, sql, params));
    }

    public int executeUpdate(Connection connection,
                              String sql,
                              Object... params) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {

            int cnt = 0;
            for (Object param : params) {

                if(param instanceof StringReader) {
                    ps.setCharacterStream(++cnt, (StringReader)param);
                    continue;
                }

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
    public int[] executeBatchUpdate(Connection connection, String sql, Object[][] batchParams) {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {

            for (Object[] batch : batchParams) {
                int cnt = 0;
                for(Object param : batch) {
                    ps.setObject(++cnt, param);
                }
                ps.addBatch();
            }

            return ps.executeBatch();
        } catch (Exception e) {
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

    public void connectNoResult(Consumer<Connection> connectionConsumer) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connectionConsumer.accept(connection);
        }
    }

    public void transactionNoResult(ConnectionConsumer connectionConsumer) throws SQLException {
        connectNoResult(connection -> {
            try (DbTransaction dbTransaction = new DbTransaction(connection)) {
                    connectionConsumer.accept(connection);
                    dbTransaction.commit();
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
    }

    public <T> T transaction(Function<Connection, T> connectionConsumer) {
        return connect(connection -> {
            try (DbTransaction dbTransaction = new DbTransaction(connection)) {
                    T result = connectionConsumer.apply(connection);
                    dbTransaction.commit();
                    return result;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        });
    }

    private String determineDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getMetaData().getDatabaseProductName();
        }
    }

    public interface RowMapper<R> {
        R map(ResultSet rs) throws Exception;
    }

    public interface ConnectionConsumer {
        void accept(Connection connection) throws Exception;
    }

}
