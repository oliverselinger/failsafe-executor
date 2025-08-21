package os.failsafe.executor.db;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.db.testcontainers.*;
import os.failsafe.executor.utils.Database;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * JUnit extension that provides database access for tests using testcontainers.
 * Supports H2, MySQL, MariaDB, PostgreSQL, and Oracle XE databases.
 */
public class TestcontainersDbExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final static Logger log = LoggerFactory.getLogger(TestcontainersDbExtension.class);

    private final TestDatabaseContainer databaseContainer;
    private Database database;
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
        database = new Database(dataSource);
        createTable();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
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

    public Database database() {
        return database;
    }

    public void createTable() {
        String createTableScript = databaseContainer.getCreateTableScript();
        String[] statements = createTableScript.split("\\r\\n\\r\\n|\\n\\n");
        
        // First drop the table if it exists
        try {
            database.execute("DROP TABLE FAILSAFE_TASK");
        }catch (Exception e) {
            // intentionally left empty
        }
        
        // Then execute each statement in the script
        for (String statement : statements) {
            if (!statement.trim().isEmpty()) {
                database.execute(statement);
            }
        }
    }

    public void truncateTable() {
        database.update(databaseContainer.getTruncateTableStatement());
    }

    public void dropTable() {
        database.execute("DROP TABLE FAILSAFE_TASK");
    }

    public void deleteColumn(String columnName) {
        database.execute("ALTER TABLE FAILSAFE_TASK DROP COLUMN " + columnName);
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
            return DatabaseType.MARIADB;
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