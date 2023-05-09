package os.failsafe.executor;

import java.time.LocalDateTime;
import java.util.Objects;

public class Task {

    private final String id;
    private final String parameter;
    private final String name;
    private final String nodeId;
    private final LocalDateTime creationTime;
    private final LocalDateTime plannedExecutionTime;
    private final LocalDateTime lockTime;
    private final ExecutionFailure executionFailure;
    private final int retryCount;
    private final Long version;

    public Task(String id, String name, String parameter, LocalDateTime plannedExecutionTime) {
        this(id, name, parameter, null, null, plannedExecutionTime, null, null, 0, 0L);
    }

    public Task(String id, String name, String parameter, LocalDateTime creationTime, LocalDateTime plannedExecutionTime, LocalDateTime lockTime, ExecutionFailure executionFailure, int retryCount, Long version) {
        this(id, name, parameter, null, creationTime, plannedExecutionTime, lockTime, executionFailure, retryCount, version);
    }

    public Task(String id, String name, String parameter, String nodeId, LocalDateTime creationTime, LocalDateTime plannedExecutionTime, LocalDateTime lockTime, ExecutionFailure executionFailure, int retryCount, Long version) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.nodeId = nodeId;
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

    public String getNodeId() { return nodeId; }

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
                ", nodeId='" + nodeId + '\'' +
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
                Objects.equals(nodeId, task.nodeId) &&
                Objects.equals(creationTime, task.creationTime) &&
                Objects.equals(plannedExecutionTime, task.plannedExecutionTime) &&
                Objects.equals(lockTime, task.lockTime) &&
                Objects.equals(executionFailure, task.executionFailure) &&
                Objects.equals(version, task.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parameter, name, nodeId, creationTime, plannedExecutionTime, lockTime, executionFailure, version);
    }
}
