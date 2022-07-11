package os.failsafe.executor;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class TaskRepository {

    private final Database database;
    private final String tableName;
    private final SystemClock systemClock;
    private final String insertStmtMysqlOrMariaDb;
    private final String insertStmtPostgres;
    private final String insertStmtOracle;
    private final String lockStmt;
    private final String unlockStmt;
    private final String saveFailureStmt;
    private final String deleteFailureStmt;
    private final String deleteStmt;
    private final String findOneStmt;
    private final String countAllStmt;
    private final String findAllNotLockedOrderedByCreatedDateStmt;
    private final String findAllPagingStmt;
    private final String updatelockTimeStmt;

    public TaskRepository(Database database, String tableName, SystemClock systemClock) {
        this.database = database;
        this.tableName = tableName;
        this.systemClock = systemClock;

        this.insertStmtMysqlOrMariaDb = String.format(//language=SQL
                "INSERT IGNORE INTO %s" +
                        " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                        " VALUES" +
                        " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableName);

        this.insertStmtPostgres = String.format(//language=SQL
                "INSERT INTO %s" +
                        " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                        " VALUES" +
                        " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        " ON CONFLICT DO NOTHING",
                tableName);

        this.insertStmtOracle = String.format(//language=SQL
                "INSERT INTO %s" +
                        " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                        " SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM DUAL" +
                        " WHERE NOT EXISTS" +
                        " (SELECT ID FROM %s WHERE ID = ?)",
                tableName, tableName);

        this.lockStmt = String.format(//language=SQL
                "UPDATE %s" +
                        " SET" +
                        " LOCK_TIME=?, VERSION=?" +
                        " WHERE ID=? AND VERSION=?",
                this.tableName);

        this.unlockStmt = String.format(//language=SQL
                "UPDATE %s" +
                        " SET" +
                        " LOCK_TIME=NULL, PLANNED_EXECUTION_TIME=?, VERSION=?" +
                        " WHERE ID=? AND VERSION=?",
                tableName);

        this.saveFailureStmt = String.format(//language=SQL
                "UPDATE %s" +
                        " SET" +
                        " LOCK_TIME=null, FAIL_TIME=?, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=?" +
                        " WHERE ID=?",
                tableName);

        this.deleteFailureStmt = String.format(//language=SQL
                "UPDATE %s" +
                        " SET" +
                        " FAIL_TIME=null, EXCEPTION_MESSAGE=null, STACK_TRACE=null, RETRY_COUNT=?, VERSION=?" +
                        " WHERE ID=? AND VERSION=?",
                tableName);

        this.deleteStmt = String.format(//language=SQL
                "DELETE FROM %s WHERE ID = ? AND VERSION = ?", tableName);

        this.findAllPagingStmt = String.format(//language=SQL
                "SELECT * FROM %s WHERE" +
                        " (NAME = ? OR ? IS NULL)" +
                        " AND (PARAMETER = ? OR ? IS NULL)" +
                        " AND ( (FAIL_TIME IS NOT NULL AND ? = 1) OR (FAIL_TIME IS NULL AND ? = 0) OR ? IS NULL )" +
                        " ORDER BY %s", tableName, database.isMysqlOrMariaDb() ? "%s LIMIT ?, ?" : "%s OFFSET ? ROWS FETCH NEXT ? ROWS ONLY");

        this.countAllStmt = String.format(//language=SQL
                "SELECT COUNT(*) FROM %s WHERE" +
                        " (NAME = ? OR ? IS NULL)" +
                        " AND (PARAMETER = ? OR ? IS NULL)" +
                        " AND ( (FAIL_TIME IS NOT NULL AND ? = 1) OR (FAIL_TIME IS NULL AND ? = 0) OR ? IS NULL )",
                tableName);

        this.findOneStmt = String.format(//language=SQL
                "SELECT * FROM %s WHERE ID = ?", tableName);

        this.findAllNotLockedOrderedByCreatedDateStmt = String.format(//language=SQL
                "SELECT * FROM %s WHERE" +
                        " FAIL_TIME IS NULL AND (LOCK_TIME IS NULL OR LOCK_TIME <= ?)" +
                        " AND PLANNED_EXECUTION_TIME <= ? AND NAME IN (%s)" +
                        " ORDER BY CREATED_DATE %s",
                tableName, "%s", database.isMysqlOrMariaDb() ? "LIMIT ?" : "FETCH FIRST ? ROWS ONLY");

        this.updatelockTimeStmt = String.format(//language=SQL
                "UPDATE %s" +
                        " SET" +
                        " LOCK_TIME=?" +
                        " WHERE ID=? AND LOCK_TIME IS NOT NULL AND VERSION=?",
                this.tableName);
    }

    String add(Task task) {
        return database.connect(connection -> this.add(connection, task));
    }

    String add(Connection connection, Task task) {
        LocalDateTime creationTime = systemClock.now();

        int count = 0;
        if (database.isOracle() || database.isH2()) {
            count = addTaskInOracle(connection, task, creationTime);
        } else if (database.isMysqlOrMariaDb()) {
            count = addTaskInMysqlOrMariaDb(connection, task, creationTime);
        } else if (database.isPostgres()) {
            count = addTaskInPostgres(connection, task, creationTime);
        }

        return count == 1 ? task.getId() : null;
    }

    private int addTaskInMysqlOrMariaDb(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        return database.insert(connection, insertStmtMysqlOrMariaDb,
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

    private int addTaskInPostgres(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        return database.insert(connection, insertStmtPostgres,
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

    private int addTaskInOracle(Connection connection, Task task, LocalDateTime creationTime) {
        ExecutionFailure executionFailure = task.getExecutionFailure();
        return database.insert(connection, insertStmtOracle,
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
        return database.selectOne(findOneStmt, this::mapToPersistentTask, id);
    }

    List<Task> findAll(String taskName, String parameter, Boolean failed, int offset, int limit, Sort... sorts) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be greater or equal 0");
        }

        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be greater than 0");
        }

        if (sorts.length == 0) {
            sorts = new Sort[]{new Sort(Sort.Field.CREATED_DATE, Sort.Direction.DESC), new Sort(Sort.Field.ID, Sort.Direction.DESC)};
        }

        String orderBys = Arrays.stream(sorts).map(Sort::toString).collect(Collectors.joining(","));
        String sql = String.format(findAllPagingStmt, orderBys);

        return database.selectAll(sql, this::mapToPersistentTask,
                taskName, taskName,
                parameter, parameter,
                failed, failed, failed,
                offset,
                limit);
    }

    int count(String taskName, String parameter, Boolean failed) {
        return database.selectOne(countAllStmt, rs -> rs.getInt(1),
                taskName, taskName,
                parameter, parameter,
                failed, failed, failed);
    }

    List<Task> lock(Connection connection, List<Task> toLock) {
        LocalDateTime lockTime = systemClock.now();
        Timestamp timestamp = Timestamp.valueOf(lockTime);

        List<Object[]> params = new ArrayList<>();
        for (Task task : toLock) {
            params.add(new Object[]{timestamp, task.getVersion() + 1, task.getId(), task.getVersion()});
        }

        int[] updateCount = database.executeBatchUpdate(connection, lockStmt, params.toArray(new Object[params.size()][]));

        List<Task> result = new ArrayList<>();
        for (int i = 0; i < updateCount.length; i++) {
            if (updateCount[i] == 1) {
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

        String selectStmt = String.format(findAllNotLockedOrderedByCreatedDateStmt, processableTasks.stream().map(s -> "?").collect(Collectors.joining(",")));

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

    void deleteFailure(Connection con, Task failed) {
        int updateCount = database.executeUpdate(con, deleteFailureStmt, failed.getRetryCount() + 1, failed.getVersion() + 1, failed.getId(), failed.getVersion());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete failure of task %s", failed.getId()));
        }
    }

    void delete(Task toDelete) {
        try {
            database.connectNoResult(con -> delete(con, toDelete));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    void delete(Connection connection, Task toDelete) {
        int deleteCount = database.executeUpdate(connection, deleteStmt, toDelete.getId(), toDelete.getVersion());

        if (deleteCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete task %s", toDelete.getId()));
        }
    }

    public List<Task> updateLockTime(List<Task> toUpdate) {
        LocalDateTime lockTime = systemClock.now();
        Timestamp timestamp = Timestamp.valueOf(lockTime);

        List<Object[]> params = new ArrayList<>();
        for (Task task : toUpdate) {
            params.add(new Object[]{timestamp, task.getId(), task.getVersion()});
        }

        int[] updateCount = database.connect(con -> database.executeBatchUpdate(con, updatelockTimeStmt, params.toArray(new Object[params.size()][])));

        List<Task> result = new ArrayList<>();
        for (int i = 0; i < updateCount.length; i++) {
            if (updateCount[i] == 1) {
                Task task = toUpdate.get(i);
                result.add(new Task(task.getId(), task.getName(), task.getParameter(), task.getCreationTime(), task.getPlannedExecutionTime(), lockTime, null, task.getRetryCount(), task.getVersion()));
            }
        }
        return result;
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
