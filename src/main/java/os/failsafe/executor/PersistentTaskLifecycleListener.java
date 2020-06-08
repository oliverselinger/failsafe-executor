package os.failsafe.executor;

public interface PersistentTaskLifecycleListener {
    void cancel(Task toCancel);

    void retry(Task toRetry);
}
