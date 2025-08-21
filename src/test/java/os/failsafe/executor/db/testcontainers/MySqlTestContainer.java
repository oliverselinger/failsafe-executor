package os.failsafe.executor.db.testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.MySQLContainer;
import os.failsafe.executor.utils.FileUtil;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;

/**
 * MySQL database container for tests.
 */
public class MySqlTestContainer implements TestDatabaseContainer {

    private final MySQLContainer<?> container;

    public MySqlTestContainer() {
        container = new MySQLContainer<>("mysql:8.0")
                .withDatabaseName("failsafe")
                .withUsername("failsafe")
                .withPassword("failsafe");
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {
        container.stop();
    }

    @Override
    public DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(container.getJdbcUrl());
        config.setUsername(container.getUsername());
        config.setPassword(container.getPassword());
        config.setDriverClassName(container.getDriverClassName());
        config.setMaximumPoolSize(getMaxPoolSize());
        
        getAdditionalConfigs().forEach(config::addDataSourceProperty);
        
        return new HikariDataSource(config);
    }

    @Override
    public String getCreateTableScript() {
        return FileUtil.readResourceFile("mysql.sql");
    }

    @Override
    public String getTruncateTableStatement() {
        return "TRUNCATE TABLE FAILSAFE_TASK";
    }

    @Override
    public int getMaxPoolSize() {
        return 5;
    }

    @Override
    public Map<String, String> getAdditionalConfigs() {
        return Collections.emptyMap();
    }
}