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
import java.util.List;
import java.util.Set;

class TaskRepository implements TaskLifecycleListener {

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
        if (database.isOracle() || database.isH2()) {
            addTaskInOracle(connection, task);
        } else if (database.isMysql()) {
            addTaskInMysql(connection, task);
        } else if (database.isPostgres()) {
            addTaskInPostgres(connection, task);
        } else {
            throw new RuntimeException("Unsupported database");
        }

        return new Task(task.getId(), task.getParameter(), task.getName(), task.getPlannedExecutionTime());
    }

    private void addTaskInMysql(Connection connection, Task task) {
        String insertStmt = "" +
                "INSERT IGNORE INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE)" +
                " VALUES" +
                " (?, ?, ?, ?, ?)";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(systemClock.now()));
    }

    private void addTaskInPostgres(Connection connection, Task task) {
        String insertStmt = "" +
                "INSERT INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE)" +
                " VALUES" +
                " (?, ?, ?, ?, ?)" +
                " ON CONFLICT DO NOTHING";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(systemClock.now()));
    }

    private void addTaskInOracle(Connection connection, Task task) {
        String insertStmt = "" +
                "INSERT INTO FAILSAFE_TASK" +
                " (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE)" +
                " SELECT ?, ?, ?, ?, ? FROM DUAL" +
                " WHERE NOT EXISTS" +
                " (SELECT ID FROM FAILSAFE_TASK WHERE ID = ?)";

        database.insert(connection, insertStmt,
                task.getId(),
                task.getName(),
                task.getParameter(),
                Timestamp.valueOf(task.getPlannedExecutionTime()),
                Timestamp.valueOf(systemClock.now()),
                task.getId());
    }

    Task findOne(String id) {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK WHERE ID = ?";
        return database.selectOne(selectStmt, this::mapToPersistentTask, id);
    }

    List<Task> findAll() {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK";
        return database.selectAll(selectStmt, this::mapToPersistentTask);
    }

    Task lock(Task toLock) {
        if (toLock.isLocked()) {
            return toLock;
        }

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
            return new Task(toLock.getId(), toLock.getParameter(), toLock.getName(), toLock.getPlannedExecutionTime(), lockTime, null, toLock.getVersion() + 1, this);
        }

        return null;
    }

    void unlock(Task toUnLock, LocalDateTime nextPlannedExecutionTime) {
        if (!toUnLock.isLocked()) {
            return;
        }

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
        String selectStmt = "" +
                "SELECT * FROM FAILSAFE_TASK" +
                " WHERE FAIL_TIME IS NULL AND (LOCK_TIME IS NULL OR LOCK_TIME <= ?)" +
                " AND PLANNED_EXECUTION_TIME <= ? AND NAME IN (?)" +
                " ORDER BY CREATED_DATE";

        if (database.isMysql()) {
            selectStmt += " LIMIT ?";
        } else {
            selectStmt += " FETCH FIRST (?) ROWS ONLY";
        }

        return database.selectAll(selectStmt, this::mapToPersistentTask,
                Timestamp.valueOf(lockTimeLessOrEqual),
                Timestamp.valueOf(plannedExecutionDateLessOrEquals),
                processableTasks.toArray(new String[processableTasks.size()]),
                limit);
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
                " FAIL_TIME=null, EXCEPTION_MESSAGE=null, STACK_TRACE=null, VERSION=?" +
                " WHERE ID=? AND VERSION=?";

        int updateCount = database.update(updateStmt, failed.getVersion() + 1, failed.getId(), failed.getVersion());

        if (updateCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete failure of task %s", failed.getId()));
        }
    }

    List<Task> findAllFailedTasks() {
        String selectStmt = "SELECT * FROM FAILSAFE_TASK WHERE FAIL_TIME IS NOT NULL";
        return database.selectAll(selectStmt, this::mapToPersistentTask);
    }

    void delete(Task toDelete) {
        String deleteStmt = "DELETE FROM FAILSAFE_TASK WHERE ID = ? AND VERSION = ?";
        int deleteCount = database.delete(deleteStmt, toDelete.getId(), toDelete.getVersion());

        if (deleteCount != 1) {
            throw new RuntimeException(String.format("Couldn't delete task %s", toDelete.getId()));
        }
    }

    Task mapToPersistentTask(ResultSet rs) {
        try {
            Timestamp lockTime = rs.getTimestamp("LOCK_TIME");

            return new Task(
                    rs.getString("ID"),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime(),
                    lockTime != null ? lockTime.toLocalDateTime() : null,
                    mapToExecutionFailure(rs),
                    rs.getLong("VERSION"),
                    this);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    ExecutionFailure mapToExecutionFailure(ResultSet rs) {
        try {
            Timestamp failTime = rs.getTimestamp("FAIL_TIME");
            String exceptionMessage = rs.getString("EXCEPTION_MESSAGE");
            String stackTrace = rs.getString("STACK_TRACE");

            if (failTime == null) {
                return null;
            }

            return new ExecutionFailure(failTime.toLocalDateTime(), exceptionMessage, stackTrace);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cancel(Task toCancel) {
        delete(toCancel);
    }

    @Override
    public void retry(Task toRetry) {
        deleteFailure(toRetry);
    }
}
