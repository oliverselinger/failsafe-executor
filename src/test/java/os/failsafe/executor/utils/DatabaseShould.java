package os.failsafe.executor.utils;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

public class DatabaseShould {

    @Test
    void throwExceptionIfDatabaseIsNotSupported() throws SQLException {
        Connection connection = Mockito.mock(Connection.class, RETURNS_DEEP_STUBS);
        when(connection.getMetaData().getDatabaseProductName()).thenReturn("UNKNOWN_DB");

        DataSource unsupportedDataSource = Mockito.mock(DataSource.class);
        when(unsupportedDataSource.getConnection()).thenReturn(connection);

        assertThrows(RuntimeException.class, () -> new Database(unsupportedDataSource));
    }
}
