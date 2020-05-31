package os.failsafe.executor;

import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.ExceptionUtils;
import os.failsafe.executor.utils.StringUtils;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.Timestamp;
import java.time.LocalDateTime;

class PersistentTask {

    private static final String SET_LOCK_TIME = "UPDATE PERSISTENT_TASK SET VERSION=?, LOCK_TIME=? WHERE VERSION=? AND ID=?";
    private static final String CLEAR_LOCK_TIME_AND_SET_NEXT_EXECUTION_TIME = "UPDATE PERSISTENT_TASK SET VERSION=?, LOCK_TIME=null, PLANNED_EXECUTION_TIME=? WHERE VERSION=? AND ID=?";
    private static final String DELETE_TASK = "DELETE FROM PERSISTENT_TASK WHERE ID=? AND VERSION=?";
    private static final String FAIL_TASK = "UPDATE PERSISTENT_TASK SET FAILED=1, FAIL_TIME=?, EXCEPTION_MESSAGE=?, STACK_TRACE=?, VERSION=? WHERE ID=? AND VERSION=?";

    private final TaskId id;
    private final String parameter;
    private final String name;
    private final LocalDateTime plannedExecutionTime;
    final LocalDateTime startTime;
    final Long version;
    private final Database database;
    private final SystemClock systemClock;

    public PersistentTask(String id, String parameter, String name, LocalDateTime plannedExecutionTime, Database database, SystemClock systemClock) {
        this(new TaskId(id), parameter, name, plannedExecutionTime, null, 0L, database, systemClock);
    }

    public PersistentTask(TaskId id, String parameter, String name, LocalDateTime plannedExecutionTime, LocalDateTime startTime, Long version, Database database, SystemClock systemClock) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.plannedExecutionTime = plannedExecutionTime;
        this.startTime = startTime;
        this.version = version;
        this.database = database;
        this.systemClock = systemClock;
    }

    PersistentTask lock(Connection connection) {
        LocalDateTime startTime = systemClock.now();

        int effectedRows = database.update(connection, SET_LOCK_TIME,
                version + 1,
                Timestamp.valueOf(startTime),
                version,
                id.id);
        if (effectedRows == 1) {
            return new PersistentTask(id, parameter, name, plannedExecutionTime, startTime, version + 1, database, systemClock);
        }

        return null;
    }

    void remove() {
        database.delete(DELETE_TASK, id.id, version);
    }

    void fail(Exception exception) {
        String message = StringUtils.abbreviate(exception.getMessage(), 1000);
        String stackTrace = ExceptionUtils.stackTraceAsString(exception);

        int updateCount = database.update(FAIL_TASK, Timestamp.valueOf(systemClock.now()), message, stackTrace, version+1, id.id, version);

        if (updateCount != 1) {
            throw new RuntimeException("Couldn't set task to failed.");
        }
    }

    void nextExecution(LocalDateTime time) {
        int effectedRows = database.update(CLEAR_LOCK_TIME_AND_SET_NEXT_EXECUTION_TIME,
                version + 1,
                Timestamp.valueOf(time),
                version,
                id.id);
        if (effectedRows != 1) {
            throw new RuntimeException("Unable to set next planned execution time");
        }
    }

    public TaskId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getParameter() {
        return parameter;
    }

    public LocalDateTime getPlannedExecutionTime() {
        return plannedExecutionTime;
    }

}
