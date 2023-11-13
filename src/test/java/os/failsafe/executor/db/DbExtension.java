package os.failsafe.executor.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.utils.Database;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.TimeZone;

public class DbExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final static Logger log = LoggerFactory.getLogger(DbExtension.class);

    private final static DatabaseTestConfig databaseTestConfig = findDatabaseConfig();

    private final TimeZone jdbcClientTimezone;

    private Database database;
    private HikariDataSource dataSource;

    public DbExtension() {
        this(null);
    }

    public DbExtension(TimeZone jdbcClientTimezone) {
        this.jdbcClientTimezone = jdbcClientTimezone;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(databaseTestConfig.jdbcUrl());
        config.setDriverClassName(databaseTestConfig.driver());
        config.setUsername(databaseTestConfig.user());
        config.setPassword(databaseTestConfig.password());
        config.setMaximumPoolSize(databaseTestConfig.maxPoolSize());

        databaseTestConfig.getAdditionalConfigs().forEach(config::addDataSourceProperty);

        dataSource = new HikariDataSource(config);

        database = new Database(dataSource, jdbcClientTimezone);

        createTable();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        databaseTestConfig.truncateTable(database);
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public Database database() {
        return database;
    }

    public void createTable() {
        databaseTestConfig.createTable(database);
    }

    public void dropTable() {
        database.execute("DROP TABLE FAILSAFE_TASK");
    }

    public void deleteColumn(String columnName) {
        database.execute("ALTER TABLE FAILSAFE_TASK DROP COLUMN " + columnName);
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        dataSource.close();
    }

    private static DatabaseTestConfig findDatabaseConfig() {
        String testDB = System.getenv("TEST_DB");

        if (testDB == null) {
            log.info("Configuring H2 database");
            return new H2DatabaseTestConfig();
        }

        switch (testDB) {
            case "POSTGRES":
                log.info("Configuring Postgres database");
                return new PostgresDatabaseTestConfig();
            case "MYSQL":
                log.info("Configuring MySql database");
                return new MySqlDatabaseTestConfig();
            case "MARIA":
                log.info("Configuring Maria database");
                return new MariaDatabaseTestConfig();
            default:
                throw new IllegalArgumentException("Unknown test database");
        }
    }
}
