package os.failsafe.executor.utils.testing;

public interface NoWaitPredicate {
    boolean shouldNotWaitForTask(String name, String taskId, String parameter);
}
