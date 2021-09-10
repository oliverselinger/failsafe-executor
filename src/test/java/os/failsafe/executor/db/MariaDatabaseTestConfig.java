package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.FileUtil;

class MariaDatabaseTestConfig implements DatabaseTestConfig {

    public void createTable(Database database) {
        String createTableSql = FileUtil.readResourceFile("mysql.sql");

        String[] split = createTableSql.split("\\n\\n");

        database.execute("DROP TABLE IF EXISTS FAILSAFE_TASK",
                split[0], split[1]);
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
        return "org.mariadb.jdbc.Driver";
    }

    public String jdbcUrl() {
        return "jdbc:mariadb://localhost:3306/failsafe";
    }

    public int maxPoolSize() {
        return 5;
    }
}
