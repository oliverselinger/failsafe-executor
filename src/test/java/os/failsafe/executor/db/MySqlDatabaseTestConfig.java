package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.FileUtil;

class MySqlDatabaseTestConfig implements DatabaseTestConfig {

    public void createTable(Database database) {
        database.execute("DROP TABLE IF EXISTS FAILSAFE_TASK");

        String createTableSql = FileUtil.readResourceFile("mysql.sql");
        String[] split = createTableSql.split("\\r\\n\\r\\n|\\n\\n");
        for (String stmt : split) {
            database.execute(stmt);
        }
    }

    public void truncateTable(Database database) {
        database.update("TRUNCATE TABLE FAILSAFE_TASK");
    }

    public String user() {
        return "failsafe";
    }

    public String password() {
        return "failsafe";
    }

    public String driver() {
        return "com.mysql.cj.jdbc.Driver";
    }

    public String jdbcUrl() {
        return "jdbc:mysql://localhost:3306/failsafe";
    }

    public int maxPoolSize() {
        return 5;
    }
}
