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
import os.failsafe.executor.utils.FileUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class H2DbExtension implements BeforeAllCallback, AfterEachCallback, AfterAllCallback {

    private HikariDataSource dataSource;

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:taskdb");
        config.setDriverClassName("org.h2.Driver");
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(1);

        dataSource = new HikariDataSource(config);

        try (Connection con = dataSource.getConnection();
             Statement statement = con.createStatement()) {

            String createTablesSql = FileUtil.readResourceFile("table.sql");
            statement.execute(createTablesSql);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        try (Connection con = dataSource.getConnection();
             Statement statement = con.createStatement()) {
            statement.executeUpdate("TRUNCATE TABLE PERSISTENT_TASK");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        dataSource.close();
    }
}