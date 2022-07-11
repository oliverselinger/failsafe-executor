package os.failsafe.executor.db;

import os.failsafe.executor.utils.Database;

import java.util.Collections;
import java.util.Map;

interface DatabaseTestConfig {
    void createTable(Database database);

    void truncateTable(Database database);

    String user();

    String password();

    String driver();

    String jdbcUrl();

    int maxPoolSize();

    default Map<String, String> getAdditionalConfigs() {
        return Collections.emptyMap();
    }
}
