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
