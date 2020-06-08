package os.failsafe.executor;

public interface TaskExecutionListener {
    void registered(String name, String id, String parameter);

    void succeeded(String name, String id, String parameter);

    void failed(String name, String id, String parameter);
}
