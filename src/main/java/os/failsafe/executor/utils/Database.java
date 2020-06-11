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

    private final boolean oracleDatabase;
    private final boolean mysqlDatabase;
    private final boolean postgresDatabase;
    private final boolean h2Database;
    private final DataSource dataSource;

    public Database(DataSource dataSource) {
        this.dataSource = dataSource;

        String databaseName = determineDatabase();
        this.oracleDatabase = databaseName.equalsIgnoreCase("Oracle");
        this.mysqlDatabase = databaseName.equalsIgnoreCase("MySQL");
        this.postgresDatabase = databaseName.equalsIgnoreCase("PostgreSQL");
        this.h2Database = databaseName.equalsIgnoreCase("H2");
    }

    public boolean isOracle() {
        return oracleDatabase;
    }

    public boolean isMysql() {
        return mysqlDatabase;
    }

    public boolean isPostgres() {
        return postgresDatabase;
    }

    public boolean isH2() {
        return h2Database;
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

    public <T> List<T> selectAll(String sql, Function<ResultSet, T> mapper, Object[] params) {
        return connect(connection -> selectAll(connection, sql, mapper, params));
    }

    public <T> List<T> selectAll(Connection connection, String sql, Function<ResultSet, T> resultSetConsumer, Object[] params) {

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
                    result.add(resultSetConsumer.apply(rs));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insert(Connection connection,
                       String sql,
                       Object... params) {
        executeUpdate(connection, sql, params);
    }

    public int update(Connection connection,
                      String sql,
                      Object... params) {
        return executeUpdate(connection, sql, params);
    }

    public int update(String sql,
                      Object... params) {
        return executeUpdate(sql, params);
    }

    public int delete(String sql,
                      Object... params) {
        return executeUpdate(sql, params);
    }

    public int executeUpdate(String sql,
                             Object... params) {
        return connect(connection -> executeUpdate(connection, sql, params));
    }

    private int executeUpdate(Connection connection,
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

    private String determineDatabase() {
        try (Connection connection = dataSource.getConnection()) {
            return connection.getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new RuntimeException("Unable to determine database product name", e);
        }
    }
}
