package os.failsafe.executor.task;

public interface TaskExecutionListener {
    void registered(String name, TaskId id, String parameter);

    void succeeded(String name, TaskId id, String parameter);
    void failed(String name, TaskId id, String parameter);
}
