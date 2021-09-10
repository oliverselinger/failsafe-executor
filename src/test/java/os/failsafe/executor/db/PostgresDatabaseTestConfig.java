package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.FileUtil;

class PostgresDatabaseTestConfig implements DatabaseTestConfig {

    public void createTable(Database database) {
        String createTableSql = FileUtil.readResourceFile("postgres.sql");

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
        return "org.postgresql.Driver";
    }

    public String jdbcUrl() {
        return "jdbc:postgresql://127.0.0.1:5432/failsafe";
    }

    public int maxPoolSize() {
        return 5;
    }
}
