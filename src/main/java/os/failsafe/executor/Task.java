package os.failsafe.executor;

import java.time.LocalDateTime;
import java.util.Objects;

public class Task {

    private final String id;
    private final String parameter;
    private final String name;
    private final LocalDateTime plannedExecutionTime;
    private final LocalDateTime lockTime;
    private final ExecutionFailure executionFailure;
    private final Long version;
    private final TaskLifecycleListener taskLifecycleListener;

    public Task(String id, String parameter, String name, LocalDateTime plannedExecutionTime) {
        this(id, parameter, name, plannedExecutionTime, null, null, 0L, null);
    }

    public Task(String id, String parameter, String name, LocalDateTime plannedExecutionTime, LocalDateTime lockTime, ExecutionFailure executionFailure, Long version, TaskLifecycleListener taskLifecycleListener) {
        this.id = id;
        this.parameter = parameter;
        this.name = name;
        this.plannedExecutionTime = plannedExecutionTime;
        this.lockTime = lockTime;
        this.executionFailure = executionFailure;
        this.version = version;
        this.taskLifecycleListener = taskLifecycleListener;
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

    public LocalDateTime getPlannedExecutionTime() {
        return plannedExecutionTime;
    }

    public LocalDateTime getLockTime() {
        return lockTime;
    }

    public ExecutionFailure getExecutionFailure() {
        return executionFailure;
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

    public boolean cancel() {
        if (isCancelable()) {
            taskLifecycleListener.cancel(this);
            return true;
        }

        return false;
    }

    public boolean isRetryable() {
        return isExecutionFailed();
    }

    public boolean retry() {
        if (isRetryable()) {
            taskLifecycleListener.retry(this);
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "Task{" +
                "id='" + id + '\'' +
                ", parameter='" + parameter + '\'' +
                ", name='" + name + '\'' +
                ", plannedExecutionTime=" + plannedExecutionTime +
                ", lockTime=" + lockTime +
                ", executionFailure=" + executionFailure +
                ", version=" + version +
                ", taskLifecycleListener=" + taskLifecycleListener +
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
                Objects.equals(plannedExecutionTime, task.plannedExecutionTime) &&
                Objects.equals(lockTime, task.lockTime) &&
                Objects.equals(executionFailure, task.executionFailure) &&
                Objects.equals(version, task.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parameter, name, plannedExecutionTime, lockTime, executionFailure, version);
    }
}
