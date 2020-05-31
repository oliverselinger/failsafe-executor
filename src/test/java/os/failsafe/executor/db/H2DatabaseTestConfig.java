package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.FileUtil;

class H2DatabaseTestConfig implements DatabaseTestConfig {

    public void createTable(Database database) {
        String createTableSql = FileUtil.readResourceFile("oracle.sql");

        database.execute("DROP TABLE IF EXISTS PERSISTENT_TASK",
                createTableSql);
    }

    public void truncateTable(Database database) {
        database.update("TRUNCATE TABLE PERSISTENT_TASK");
    }

    public String user() {
        return "sa";
    }

    public String password() {
        return "";
    }

    public String driver() {
        return "org.h2.Driver";
    }

    public String jdbcUrl() {
        return "jdbc:h2:mem:taskdb";
    }

    public int maxPoolSize() {
        return 1;
    }
}
