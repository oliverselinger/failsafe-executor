package os.failsafe.executor.db;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.db.testcontainers.H2TestContainer;
import os.failsafe.executor.db.testcontainers.MariaDbTestContainer;
import os.failsafe.executor.db.testcontainers.MySqlTestContainer;
import os.failsafe.executor.db.testcontainers.OracleTestContainer;
import os.failsafe.executor.db.testcontainers.PostgresTestContainer;
import os.failsafe.executor.db.testcontainers.TestDatabaseContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * JUnit extension that provides database access for tests using testcontainers.
 * Supports H2, MySQL, MariaDB, PostgreSQL, and Oracle XE databases.
 */
public class TestcontainersDbExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final static Logger log = LoggerFactory.getLogger(TestcontainersDbExtension.class);

    private final TestDatabaseContainer databaseContainer;
    private DataSource dataSource;

    /**
     * Creates a new TestcontainersDbExtension with the specified database type.
     * 
     * @param databaseType the type of database to use
     */
    public TestcontainersDbExtension(DatabaseType databaseType) {
        this.databaseContainer = createDatabaseContainer(databaseType);
    }

    /**
     * Creates a new TestcontainersDbExtension with the database type specified by the TEST_DB environment variable.
     * If TEST_DB is not set, H2 is used as the default.
     */
    public TestcontainersDbExtension() {
        this(findDatabaseType());
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws SQLException {
        databaseContainer.start();
        dataSource = databaseContainer.createDataSource();
        createTable();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws SQLException {
        truncateTable();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                log.error("Error closing datasource", e);
            }
        }
        databaseContainer.stop();
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public void createTable() throws SQLException {
        String createTableScript = databaseContainer.getCreateTableScript();
        String[] statements = createTableScript.split("\\r\\n\\r\\n|\\n\\n");
        
        // First drop the table if it exists
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement("DROP TABLE FAILSAFE_TASK")) {
            stmt.execute();
        } catch (Exception e) {
            // intentionally left empty
        }
        
        // Then execute each statement in the script
        for (String statement : statements) {
            if (!statement.trim().isEmpty()) {
                try (Connection connection = dataSource.getConnection();
                     PreparedStatement stmt = connection.prepareStatement(statement)) {
                    stmt.execute();
                }
            }
        }
    }

    public void truncateTable() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(databaseContainer.getTruncateTableStatement())) {
            stmt.execute();
        }
    }

    private static TestDatabaseContainer createDatabaseContainer(DatabaseType databaseType) {
        switch (databaseType) {
            case H2:
                log.info("Configuring H2 database");
                return new H2TestContainer();
            case POSTGRES:
                log.info("Configuring PostgreSQL database");
                return new PostgresTestContainer();
            case MYSQL:
                log.info("Configuring MySQL database");
                return new MySqlTestContainer();
            case MARIADB:
                log.info("Configuring MariaDB database");
                return new MariaDbTestContainer();
            case ORACLE:
                log.info("Configuring Oracle database");
                return new OracleTestContainer();
            default:
                throw new IllegalArgumentException("Unknown database type: " + databaseType);
        }
    }

    private static DatabaseType findDatabaseType() {
        String testDB = System.getenv("TEST_DB");

        if (testDB == null) {
            log.info("TEST_DB environment variable not set, using H2 database");
            return DatabaseType.H2;
        }

        try {
            return DatabaseType.valueOf(testDB);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown TEST_DB value: {}, using H2 database", testDB);
            return DatabaseType.H2;
        }
    }

    /**
     * Enum representing the supported database types.
     */
    public enum DatabaseType {
        H2,
        POSTGRES,
        MYSQL,
        MARIADB,
        ORACLE
    }
}