package os.failsafe.executor.db.testcontainers;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Interface for test database containers.
 */
public interface TestDatabaseContainer {
    
    /**
     * Start the container.
     */
    void start();
    
    /**
     * Stop the container.
     */
    void stop();
    
    /**
     * Create a DataSource for the container.
     * 
     * @return DataSource connected to the container
     */
    DataSource createDataSource();
    
    /**
     * Get the SQL script for creating the required tables.
     * 
     * @return SQL script for creating tables
     */
    String getCreateTableScript();
    
    /**
     * Get the SQL statement for truncating the table.
     * 
     * @return SQL statement for truncating the table
     */
    String getTruncateTableStatement();
    
    /**
     * Get the maximum connection pool size.
     * 
     * @return maximum connection pool size
     */
    int getMaxPoolSize();
    
    /**
     * Get additional configuration properties for the data source.
     * 
     * @return map of additional configuration properties
     */
    Map<String, String> getAdditionalConfigs();
}