package os.failsafe.executor.task;

public interface PersistentTaskLifecycleListener {
    void cancel(PersistentTask toCancel);

    void retry(PersistentTask toRetry);
}
