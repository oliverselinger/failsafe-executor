package os.failsafe.executor.task;

import os.failsafe.executor.utils.Database;

import java.time.LocalDateTime;

public class FailedTask {

    private static final String DELETE_TASK = "DELETE FROM PERSISTENT_TASK WHERE ID=? AND VERSION=?";
    private static final String RETRY_TASK = "UPDATE PERSISTENT_TASK SET LOCK_TIME=null, FAILED=0, FAIL_TIME=null, EXCEPTION_MESSAGE=null, STACK_TRACE=null, VERSION=? WHERE ID=? AND VERSION=?";

    private final TaskId id;
    private final String parameter;
    private final String name;
    //TODO unused
    private final LocalDateTime startTime;
    private final Long version;
    private final LocalDateTime failTime;
    private final String exceptionMessage;
    private final String stackTrace;
    private final Database database;

    public FailedTask(TaskId id, String parameter, String name, LocalDateTime startTime, Long version, LocalDateTime failTime, String exceptionMessage, String stackTrace, Database database) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.startTime = startTime;
        this.version = version;
        this.failTime = failTime;
        this.exceptionMessage = exceptionMessage;
        this.stackTrace = stackTrace;
        this.database = database;
    }

    public void cancel() {
        database.delete(DELETE_TASK, id.id, version);
    }

    public void retry() {
        int updateCount = database.update(RETRY_TASK, version + 1, id.id, version);

        if (updateCount != 1) {
            throw new RuntimeException("Couldn't retry task.");
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

    public LocalDateTime getFailTime() {
        return failTime;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public String getStackTrace() {
        return stackTrace;
    }
}
