package os.failsafe.executor.utils;

import java.sql.Connection;
import java.sql.SQLException;

public class Transaction implements AutoCloseable {

    private final Connection connection;
    private final boolean originalAutoCommit;
    private boolean committed;

    public Transaction(Connection connection) throws SQLException {
        this.connection = connection;
        this.originalAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        connection.commit();
        committed = true;
    }

    @Override
    public void close() throws SQLException {
        if (!committed) {
            connection.rollback();
        }

        connection.setAutoCommit(originalAutoCommit);
    }
}
