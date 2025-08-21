package os.failsafe.executor.db.testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import os.failsafe.executor.utils.FileUtil;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Map;

/**
 * H2 database container for tests.
 * Note: H2 doesn't actually need a container as it's in-memory, but we use the same interface
 * for consistency with other database types.
 */
public class H2TestContainer implements TestDatabaseContainer {

    @Override
    public void start() {
        // No need to start H2
    }

    @Override
    public void stop() {
        // No need to stop H2
    }

    @Override
    public DataSource createDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:taskdb;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        config.setDriverClassName("org.h2.Driver");
        config.setMaximumPoolSize(getMaxPoolSize());
        
        getAdditionalConfigs().forEach(config::addDataSourceProperty);
        
        return new HikariDataSource(config);
    }

    @Override
    public String getCreateTableScript() {
        return FileUtil.readResourceFile("oracle.sql");
    }

    @Override
    public String getTruncateTableStatement() {
        return "TRUNCATE TABLE FAILSAFE_TASK";
    }

    @Override
    public int getMaxPoolSize() {
        return 2;
    }

    @Override
    public Map<String, String> getAdditionalConfigs() {
        return Collections.emptyMap();
    }
}