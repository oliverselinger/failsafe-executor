package os.failsafe.executor;

import os.failsafe.executor.task.FailedTask;
import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

class PersistentTasks {

    private static final String INSERT_TASK_POSTGRES = "INSERT INTO PERSISTENT_TASK (ID,NAME,PARAMETER,PLANNED_EXECUTION_TIME,CREATED_DATE) VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING";
    private static final String INSERT_TASK_MYSQL = "INSERT IGNORE INTO PERSISTENT_TASK (ID,NAME,PARAMETER,PLANNED_EXECUTION_TIME,CREATED_DATE) VALUES (?,?,?,?,?)";
    private static final String INSERT_TASK_ORACLE = "INSERT INTO PERSISTENT_TASK (ID,NAME,PARAMETER,PLANNED_EXECUTION_TIME,CREATED_DATE) SELECT ?, ?, ?, ?, ? FROM DUAL WHERE NOT EXISTS (SELECT ID FROM PERSISTENT_TASK WHERE ID = ?)";
    private static final String QUERY_ALL = "SELECT * FROM PERSISTENT_TASK";
    private static final String QUERY_ONE = QUERY_ALL + " WHERE ID=?";
    private static final String QUERY_ALL_FAILED = QUERY_ALL + " WHERE FAILED=1";
    private static final String QUERY_ONE_FAILED = QUERY_ONE + " AND FAILED=1";

    private final Database database;
    private final SystemClock systemClock;

    public PersistentTasks(Database database, SystemClock systemClock) {
        this.database = database;
        this.systemClock = systemClock;
    }

    PersistentTask create(TaskInstance task) {
        return database.connect(connection -> this.create(connection, task));
    }

    PersistentTask create(Connection connection, TaskInstance task) {
        if (database.isOracle() || database.isH2()) {
            database.insert(connection, INSERT_TASK_ORACLE,
                    task.id,
                    task.name,
                    task.parameter,
                    task.plannedExecutionTime,
                    Timestamp.valueOf(systemClock.now()),
                    task.id);
        }

        if (database.isMysql()) {
            database.insert(connection, INSERT_TASK_MYSQL,
                    task.id,
                    task.name,
                    task.parameter,
                    task.plannedExecutionTime,
                    Timestamp.valueOf(systemClock.now()));
        }

        if (database.isPostgres()) {
            database.insert(connection, INSERT_TASK_POSTGRES,
                    task.id,
                    task.name,
                    task.parameter,
                    task.plannedExecutionTime,
                    Timestamp.valueOf(systemClock.now()));
        }

        return new PersistentTask(task.id, task.parameter, task.name, task.plannedExecutionTime, database, systemClock);
    }

    PersistentTask findOne(TaskId id) {
        return database.selectOne(QUERY_ONE, this::mapToPersistentTask, id.id);
    }

    List<FailedTask> failedTasks() {
        return database.selectAll(QUERY_ALL_FAILED, this::mapToFailedTask);
    }

    Optional<FailedTask> failedTask(TaskId taskId) {
        return Optional.ofNullable(database.selectOne(QUERY_ONE_FAILED, this::mapToFailedTask, taskId.id));
    }

    PersistentTask mapToPersistentTask(ResultSet rs) {
        try {
            Timestamp pickTime = rs.getTimestamp("LOCK_TIME");

            return new PersistentTask(
                    new TaskId(rs.getString("ID")),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime(),
                    pickTime != null ? pickTime.toLocalDateTime() : null,
                    rs.getLong("VERSION"),
                    database,
                    systemClock);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    FailedTask mapToFailedTask(ResultSet rs) {
        try {
            Timestamp pickTime = rs.getTimestamp("LOCK_TIME");
            Timestamp failTime = rs.getTimestamp("FAIL_TIME");

            return new FailedTask(
                    new TaskId(rs.getString("ID")),
                    rs.getString("PARAMETER"),
                    rs.getString("NAME"),
                    pickTime != null ? pickTime.toLocalDateTime() : null,
                    rs.getLong("VERSION"),
                    failTime != null ? failTime.toLocalDateTime() : null,
                    rs.getString("EXCEPTION_MESSAGE"),
                    rs.getString("STACK_TRACE"),
                    database);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
