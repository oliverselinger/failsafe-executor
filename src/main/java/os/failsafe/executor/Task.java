package os.failsafe.executor;

import java.time.LocalDateTime;
import java.util.Objects;

public class Task {

    private final String id;
    private final String parameter;
    private final String name;
    private final LocalDateTime creationTime;
    private final LocalDateTime plannedExecutionTime;
    private final LocalDateTime lockTime;
    private final ExecutionFailure executionFailure;
    private final int retryCount;
    private final Long version;

    public Task(String id, String parameter, String name, LocalDateTime plannedExecutionTime) {
        this(id, parameter, name, null, plannedExecutionTime, null, null, 0, 0L);
    }

    public Task(String id, String parameter, String name, LocalDateTime creationTime, LocalDateTime plannedExecutionTime, LocalDateTime lockTime, ExecutionFailure executionFailure, int retryCount, Long version) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.creationTime = creationTime;
        this.plannedExecutionTime = plannedExecutionTime;
        this.lockTime = lockTime;
        this.executionFailure = executionFailure;
        this.retryCount = retryCount;
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getParameter() {
        return parameter;
    }

    public LocalDateTime getCreationTime() {
        return creationTime;
    }

    public LocalDateTime getPlannedExecutionTime() {
        return plannedExecutionTime;
    }

    public LocalDateTime getLockTime() {
        return lockTime;
    }

    public ExecutionFailure getExecutionFailure() {
        return executionFailure;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Long getVersion() {
        return version;
    }

    public boolean isLocked() {
        return lockTime != null;
    }

    public boolean isExecutionFailed() {
        return executionFailure != null;
    }

    public boolean isCancelable() {
        return !isLocked();
    }

    public boolean isRetryable() {
        return isExecutionFailed();
    }

    @Override
    public String toString() {
        return "Task{" +
                "id='" + id + '\'' +
                ", parameter='" + parameter + '\'' +
                ", name='" + name + '\'' +
                ", creationTime=" + creationTime +
                ", plannedExecutionTime=" + plannedExecutionTime +
                ", lockTime=" + lockTime +
                ", executionFailure=" + executionFailure +
                ", version=" + version +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equals(id, task.id) &&
                Objects.equals(parameter, task.parameter) &&
                Objects.equals(name, task.name) &&
                Objects.equals(creationTime, task.creationTime) &&
                Objects.equals(plannedExecutionTime, task.plannedExecutionTime) &&
                Objects.equals(lockTime, task.lockTime) &&
                Objects.equals(executionFailure, task.executionFailure) &&
                Objects.equals(version, task.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parameter, name, creationTime, plannedExecutionTime, lockTime, executionFailure, version);
    }
}
