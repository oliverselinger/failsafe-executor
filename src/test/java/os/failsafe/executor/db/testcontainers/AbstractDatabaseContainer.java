package os.failsafe.executor.db.testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Abstract base class for database containers used in tests.
 */
public abstract class AbstractDatabaseContainer<T extends JdbcDatabaseContainer<?>> {
    
    protected T container;
    
    /**
     * Start the container.
     */
    public void start() {
        container.start();
    }
    
    /**
     * Stop the container.
     */
    public void stop() {
        container.stop();
    }
    
    /**
     * Create a DataSource for the container.
     * 
     * @return DataSource connected to the container
     */
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
    
    /**
     * Get the SQL script for creating the required tables.
     * 
     * @return SQL script for creating tables
     */
    public abstract String getCreateTableScript();
    
    /**
     * Get the SQL statement for truncating the table.
     * 
     * @return SQL statement for truncating the table
     */
    public abstract String getTruncateTableStatement();
    
    /**
     * Get the maximum connection pool size.
     * 
     * @return maximum connection pool size
     */
    public abstract int getMaxPoolSize();
    
    /**
     * Get additional configuration properties for the data source.
     * 
     * @return map of additional configuration properties
     */
    public abstract Map<String, String> getAdditionalConfigs();
}