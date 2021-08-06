package os.failsafe.executor;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.StringUtils;
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

    private final Database database;
    private final SystemClock systemClock;

    public TaskRepository(Database database, SystemClock systemClock) {
        this.database = database;
        this.systemClock = systemClock;
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
        String insertStmt = "" +
                "INSERT IGNORE INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " VALUES" +
                " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null, null, null, null, 0, 0);
    }

    private void addTaskInPostgres(Connection connection, Task task, LocalDateTime creationTime) {
        String insertStmt = "" +
                "INSERT INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " VALUES" +
                " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT DO NOTHING";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null, null, null, null, 0, 0);
    }

    private void addTaskInOracle(Connection connection, Task task, LocalDateTime creationTime) {
        String insertStmt = "" +
                "INSERT INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, LOCK_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE, RETRY_COUNT, VERSION)" +
                " SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM DUAL" +
                " WHERE NOT EXISTS" +
                " (SELECT ID FROM FAILSAFE_TASK WHERE ID = ?)";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(creationTime),
                null, null, null, null, 0, 0,
                task.getId());
    }

    Task findOne(String id) {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK WHERE ID = ?";
        return database.selectOne(selectStmt, this::mapToPersistentTask, id);
    }

    List<Task> findAll() {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK";
        return database.selectAll(selectStmt, this::mapToPersistentTask, null);
    }

    Task lock(Task toLock) {
        String updateStmt = "" +
                "UPDATE FAILSAFE_TASK" +
                " SET" +
                " LOCK_TIME=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?";

        LocalDateTime lockTime = systemClock.now();
        int updateCount = database.update(updateStmt,
                Timestamp.valueOf(lockTime),
                toLock.getVersion() + 1,
                toLock.getId(),
                toLock.getVersion());

        if (updateCount == 1) {
            return new Task(toLock.getId(), toLock.getName(), toLock.getParameter(), toLock.getCreationTime(), toLock.getPlannedExecutionTime(), lockTime, null, toLock.getRetryCount(), toLock.getVersion() + 1);
        }

        return null;
    }

    void unlock(Task toUnLock, LocalDateTime nextPlannedExecutionTime) {
        String updateStmt = "" +
                "UPDATE FAILSAFE_TASK" +
                " SET" +
                " LOCK_TIME=NULL, PLANNED_EXECUTION_TIME=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?";

        int effectedRows = database.update(updateStmt,
                Timestamp.valueOf(nextPlannedExecutionTime),
                toUnLock.getVersion() + 1,
                toUnLock.getId(),
                toUnLock.getVersion());

        if (effectedRows != 1) {
            throw new RuntimeException(String.format("Could not unlock task %s", toUnLock.getId()));
        }
    }

    List<Task> findAllNotLockedOrderedByCreatedDate(Set<String> processableTasks, LocalDateTime plannedExecutionDateLessOrEquals, LocalDateTime lockTimeLessOrEqual, int limit) {
        if (processableTasks.isEmpty()) {
            return Collections.emptyList();
        }

        String whereIn = "AND NAME IN (" + processableTasks.stream().map(s -> "?").collect(Collectors.joining(",")) + ")";

        String selectStmt = "" +
                "SELECT * FROM FAILSAFE_TASK" +
                " WHERE FAIL_TIME IS NULL AND (LOCK_TIME IS NULL OR LOCK_TIME <= ?)" +
                " AND PLANNED_EXECUTION_TIME <= ? " + whereIn +
                " ORDER BY CREATED_DATE";

        if (database.isMysqlOrMariaDb()) {
            selectStmt += " LIMIT ?";
        } else {
            selectStmt += " FETCH FIRST (?) ROWS ONLY";
        }

        List<Object> params = new ArrayList<>();
        params.add(Timestamp.valueOf(lockTimeLessOrEqual));
        params.add(Timestamp.valueOf(plannedExecutionDateLessOrEquals));
        params.addAll(processableTasks);
        params.add(limit);

        return database.selectAll(selectStmt, this::mapToPersistentTask, params.toArray());
    }

    void saveFailure(Task failed, Exception exception) {
        String message = StringUtils.abbreviate(exception.getMessage(), 1000);
        String stackTrace = ExceptionUtils.stackTraceAsString(exception);

        String updateStmt = "" +
                "UPDATE FAILSAFE_TASK" +
                " SET" +
                " LOCK_TIME=null, FAIL_TIME=?, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=?" +
                " WHERE ID=?";

        int updateCount = database.update(updateStmt,
                Timestamp.valueOf(systemClock.now()),
                message,
                stackTrace,
                failed.getVersion() + 1,
                failed.getId());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't save failure to task %s", failed.getId()));
        }
    }

    void deleteFailure(Task failed) {
        String updateStmt = "" +
                "UPDATE FAILSAFE_TASK" +
                " SET" +
                " FAIL_TIME=null, EXCEPTION_MESSAGE=null, STACK_TRACE=null, RETRY_COUNT=?, VERSION=?" +
                " WHERE ID=? AND VERSION=?";

        int updateCount = database.update(updateStmt, failed.getRetryCount() + 1, failed.getVersion() + 1, failed.getId(), failed.getVersion());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete failure of task %s", failed.getId()));
        }
    }

    List<Task> findAllFailedTasks() {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK WHERE FAIL_TIME IS NOT NULL ORDER BY CREATED_DATE DESC";
        return database.selectAll(selectStmt, this::mapToPersistentTask, null);
    }

    void delete(Task toDelete) {
        String deleteStmt = "DELETE FROM FAILSAFE_TASK WHERE ID = ? AND VERSION = ?";
        int deleteCount = database.delete(deleteStmt, toDelete.getId(), toDelete.getVersion());

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
