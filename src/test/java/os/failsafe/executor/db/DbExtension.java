/*******************************************************************************
 * MIT License
 *
 * Copyright (c) 2020 Oliver Selinger
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package os.failsafe.executor.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.utils.Database;

import javax.sql.DataSource;

public class DbExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private final static Logger log = LoggerFactory.getLogger(DbExtension.class);

    private final static DatabaseTestConfig databaseTestConfig = findDatabaseConfig();

    private HikariDataSource dataSource;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(databaseTestConfig.jdbcUrl());
        config.setDriverClassName(databaseTestConfig.driver());
        config.setUsername(databaseTestConfig.user());
        config.setPassword(databaseTestConfig.password());
        config.setMaximumPoolSize(databaseTestConfig.maxPoolSize());

        dataSource = new HikariDataSource(config);

        Database.run(dataSource, databaseTestConfig::createTable);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        Database.run(dataSource, databaseTestConfig::truncateTable);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        dataSource.close();
    }

    private static DatabaseTestConfig findDatabaseConfig() {
        String testDB = System.getenv("TEST_DB");

        if (testDB == null) {
            log.info("Configuring H2 database");
            return new H2DatabaseTestConfig();
        }

        switch (testDB) {
            case "POSTGRES":
                log.info("Configuring Postgres database");
                return new PostgresDatabaseTestConfig();
            case "MYSQL":
                log.info("Configuring MySql database");
                return new MySqlDatabaseTestConfig();
            default:
                throw new IllegalArgumentException("Unknown test database");
        }
    }
}