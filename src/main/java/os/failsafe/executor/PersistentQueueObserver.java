package os.failsafe.executor;

public interface PersistentQueueObserver {

    void onPeek(int limit, int selected, int locked);
}
