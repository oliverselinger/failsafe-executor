package os.failsafe.executor;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class TaskRepository {

    public static final int DEFAULT_FIND_FETCH_LIMIT = 100;
    private final Database database;
    private final String tableName;
    private final SystemClock systemClock;
    private final String insertStmtOracle;
    private final String insertStmtMysqlOrMariaDb;
    private final String insertStmtPostgres;
    private final String selectStmtFindOne;
    private final String selectStmtFindAll;
    private final String selectStmtFindAllPaging;
    private final String lockStmt;
    private final String unlockStmt;
    private final String saveFailureStmt;
    private final String deleteFailureStmt;
    private final String selectStmtAllFailedTasks;
    private final String selectStmtAllFailedTasksPaging;
    private final String deleteTaskStmt;
    private final String selectNotLockedTasksStmt;

    public TaskRepository(Database database, String tableName, SystemClock systemClock) {
        this.database = database;
        this.tableName = tableName;
        this.systemClock = systemClock;
        insertStmtOracle = String.format("" +
                "INSERT INTO %s" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM DUAL" +
                " WHERE NOT EXISTS" +
                " (SELECT ID FROM %s WHERE ID = ?)", this.tableName, this.tableName);
        insertStmtMysqlOrMariaDb = String.format("" +
                "INSERT IGNORE INTO %s" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " VALUES" +
                " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", this.tableName);
        insertStmtPostgres = String.format("" +
                "INSERT INTO %s" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " VALUES" +
                " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT DO NOTHING", this.tableName);
        selectStmtFindOne = String.format("SELECT * FROM %s WHERE ID = ?", this.tableName);
        selectStmtFindAll = String.format("SELECT * FROM %s ORDER BY CREATED_DATE DESC, ID DESC %s", this.tableName, database.isMysqlOrMariaDb() ? "LIMIT ?" : "FETCH FIRST (?) ROWS ONLY");
        selectStmtFindAllPaging = String.format("SELECT * FROM %s ORDER BY CREATED_DATE DESC, ID DESC %s", this.tableName, database.isMysqlOrMariaDb() ? "LIMIT ?, ?" : "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");
        lockStmt = String.format("" +
                "UPDATE %s" +
                " SET" +
                " LOCK_TIME=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?", this.tableName);
        unlockStmt = String.format("" +
                "UPDATE %s" +
                " SET" +
                " LOCK_TIME=NULL, PLANNED_EXECUTION_TIME=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?", this.tableName);
        saveFailureStmt = String.format("" +
                "UPDATE %s" +
                " SET" +
                " LOCK_TIME=null, FAIL_TIME=?, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=?" +
                " WHERE ID=?", this.tableName);
        deleteFailureStmt = String.format("" +
                "UPDATE %s" +
                " SET" +
                " FAIL_TIME=null, EXCEPTION_MESSAGE=null, STACK_TRACE=null, RETRY_COUNT=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?", this.tableName);
        selectStmtAllFailedTasks = String.format("SELECT * FROM %s WHERE FAIL_TIME IS NOT NULL ORDER BY FAIL_TIME DESC, ID DESC %s", this.tableName, database.isMysqlOrMariaDb() ? "LIMIT ?" : "FETCH FIRST (?) ROWS ONLY");
        selectStmtAllFailedTasksPaging = String.format("SELECT * FROM %s WHERE FAIL_TIME IS NOT NULL ORDER BY FAIL_TIME DESC, ID DESC %s", this.tableName, database.isMysqlOrMariaDb() ? "LIMIT ?, ?" : "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");
        deleteTaskStmt = String.format("DELETE FROM %s WHERE ID = ? AND VERSION = ?", this.tableName);
        selectNotLockedTasksStmt = String.format("" +
                "SELECT * FROM %s" +
                " WHERE FAIL_TIME IS NULL AND (LOCK_TIME IS NULL OR LOCK_TIME <= ?)" +
                " AND PLANNED_EXECUTION_TIME <= ? AND NAME IN (%s)" +
                " ORDER BY CREATED_DATE %s", tableName, "%s", database.isMysqlOrMariaDb() ? " LIMIT ?" : " FETCH FIRST (?) ROWS ONLY");
    }

    Task add(Task task) {
        return database.connect(connection -> this.add(connection, task));
    }

    Task add(Connection connection, Task task) {
        LocalDateTime creationTime = systemClock.now();

        if (database.isOracle() || database.isH2()) {
            addTaskInOracle(connection, task, creationTime);
        } else if (database.isMysqlOrMariaDb()) {
            addTaskInMysqlOrMariaDb(connection, task, creationTime);
        } else if (database.isPostgres()) {
            addTaskInPostgres(connection, task, creationTime);
        }

        return new Task(task.getId(), task.getName(), task.getParameter(), creationTime, task.getPlannedExecutionTime(), null, null, 0, 0L);
    }

    private void addTaskInMysqlOrMariaDb(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        database.insert(connection, insertStmtMysqlOrMariaDb,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null,
                executionFailure != null ? Timestamp.valueOf(executionFailure.getFailTime()) : null,
                executionFailure != null ? executionFailure.getExceptionMessage() : null,
                executionFailure != null ? executionFailure.getStackTrace() : null,
                task.getRetryCount(),
                task.getVersion());
    }

    private void addTaskInPostgres(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        database.insert(connection, insertStmtPostgres,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null,
                executionFailure != null ? Timestamp.valueOf(executionFailure.getFailTime()) : null,
                executionFailure != null ? executionFailure.getExceptionMessage() : null,
                executionFailure != null ? executionFailure.getStackTrace() : null,
                task.getRetryCount(),
                task.getVersion());
    }

    private void addTaskInOracle(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        database.insert(connection, insertStmtOracle,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null,
                executionFailure != null ? Timestamp.valueOf(executionFailure.getFailTime()) : null,
                executionFailure != null ? executionFailure.getExceptionMessage() : null,
                executionFailure != null ? executionFailure.getStackTrace() : null,
                task.getRetryCount(),
                task.getVersion(),
                task.getId());
    }

    Task findOne(String id) {
        return database.selectOne(selectStmtFindOne, this::mapToPersistentTask, id);
    }

    List<Task> findAll() {
        return database.selectAll(selectStmtFindAll, this::mapToPersistentTask, new Object[] {DEFAULT_FIND_FETCH_LIMIT});
    }

    List<Task> findAll(int offset, int limit) {
        return database.selectAll(selectStmtFindAllPaging, this::mapToPersistentTask, new Object[] {offset, limit});
    }

    List<Task> lock(Connection trx, List<Task> toLock) {
        LocalDateTime lockTime = systemClock.now();
        Timestamp timestamp = Timestamp.valueOf(lockTime);

        Object[][] entries = new Object[toLock.size()][];

        for (int i = 0, toLockSize = toLock.size(); i < toLockSize; i++) {
            Task task = toLock.get(i);
            Object[] entry = {timestamp, task.getVersion() + 1, task.getId(), task.getVersion()};
            entries[i] = entry;
        }

        int[] updateCount = database.executeBatchUpdate(trx, lockStmt, entries);

        List<Task> result = new ArrayList<>();
        for (int i = 0; i < updateCount.length; i++) {
            int value = updateCount[i];
            if (value == 1) {
                Task task = toLock.get(i);
                result.add(new Task(task.getId(), task.getName(), task.getParameter(), task.getCreationTime(), task.getPlannedExecutionTime(), lockTime, null, task.getRetryCount(), task.getVersion() + 1));
            }
        }
        return result;
    }

    void unlock(Task toUnLock, LocalDateTime nextPlannedExecutionTime) {
        int effectedRows = database.update(unlockStmt,
                Timestamp.valueOf(nextPlannedExecutionTime),
                toUnLock.getVersion() + 1,
                toUnLock.getId(),
                toUnLock.getVersion());

        if (effectedRows != 1) {
            throw new RuntimeException(String.format("Could not unlock task %s", toUnLock.getId()));
        }
    }

    List<Task> findAllNotLockedOrderedByCreatedDate(Connection connection, Set<String> processableTasks, LocalDateTime plannedExecutionDateLessOrEquals, LocalDateTime lockTimeLessOrEqual, int limit) {
        if (processableTasks.isEmpty()) {
            return Collections.emptyList();
        }

        String selectStmt = String.format(selectNotLockedTasksStmt, processableTasks.stream().map(s -> "?").collect(Collectors.joining(",")));

        List<Object> params = new ArrayList<>();
        params.add(Timestamp.valueOf(lockTimeLessOrEqual));
        params.add(Timestamp.valueOf(plannedExecutionDateLessOrEquals));
        params.addAll(processableTasks);
        params.add(limit);

        return database.selectAll(connection, selectStmt, this::mapToPersistentTask, params.toArray());
    }

    void saveFailure(Task failed, ExecutionFailure executionFailure) {
        int updateCount = database.update(saveFailureStmt,
                Timestamp.valueOf(executionFailure.getFailTime()),
                executionFailure.getExceptionMessage(),
                executionFailure.getStackTrace(),
                failed.getVersion() + 1,
                failed.getId());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't save failure to task %s", failed.getId()));
        }
    }

    void deleteFailure(Task failed) {
        int updateCount = database.update(deleteFailureStmt, failed.getRetryCount() + 1, failed.getVersion() + 1, failed.getId(), failed.getVersion());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete failure of task %s", failed.getId()));
        }
    }

    List<Task> findAllFailedTasks() {
        return database.selectAll(selectStmtAllFailedTasks, this::mapToPersistentTask, new Object[] {DEFAULT_FIND_FETCH_LIMIT});
    }

    List<Task> findAllFailedTasks(int offset, int limit) {
        return database.selectAll(selectStmtAllFailedTasksPaging, this::mapToPersistentTask, new Object[] {offset, limit});
    }

    void delete(Task toDelete) {
        try {
            database.connectNoResult(con -> delete(con, toDelete));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void delete(Connection connection, Task toDelete) {
        int deleteCount = database.executeUpdate(connection, deleteTaskStmt, toDelete.getId(), toDelete.getVersion());

        if (deleteCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete task %s", toDelete.getId()));
        }
    }

    Task mapToPersistentTask(ResultSet rs) throws SQLException {
        Timestamp lockTime = rs.getTimestamp("LOCK_TIME");

        return new Task(
                rs.getString("ID"),
                rs.getString("NAME"), rs.getString("PARAMETER"),
                rs.getTimestamp("CREATED_DATE").toLocalDateTime(),
                rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime(),
                lockTime != null ? lockTime.toLocalDateTime() : null,
                mapToExecutionFailure(rs),
                rs.getInt("RETRY_COUNT"),
                rs.getLong("VERSION"));
    }

    ExecutionFailure mapToExecutionFailure(ResultSet rs) throws SQLException {
        Timestamp failTime = rs.getTimestamp("FAIL_TIME");
        String exceptionMessage = rs.getString("EXCEPTION_MESSAGE");
        String stackTrace = rs.getString("STACK_TRACE");

        if (failTime == null) {
            return null;
        }

        return new ExecutionFailure(failTime.toLocalDateTime(), exceptionMessage, stackTrace);
    }
}
