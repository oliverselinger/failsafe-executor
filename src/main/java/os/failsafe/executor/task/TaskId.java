package os.failsafe.executor.task;

import java.util.Objects;

public class TaskId {

    public final String id;

    public TaskId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "PersistentTaskId{" +
                "id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskId that = (TaskId) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
