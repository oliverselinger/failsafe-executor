package os.failsafe.executor;

public interface TaskLifecycleListener {
    void cancel(Task toCancel);

    void retry(Task toRetry);
}
