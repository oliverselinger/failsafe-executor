package os.failsafe.executor;

public interface TaskExecutionListener {
    void persisting(String name, String id, String parameter);

    void retrying(String name, String id, String parameter);

    void succeeded(String name, String id, String parameter);

    void failed(String name, String id, String parameter, Exception exception);
}
