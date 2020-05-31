package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;

interface DatabaseTestConfig {
    void createTable(Database database);

    void truncateTable(Database database);

    String user();

    String password();

    String driver();

    String jdbcUrl();

    int maxPoolSize();
}
