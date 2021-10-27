package os.failsafe.executor;

import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class PersistentQueue {

    private final Database database;
    private final SystemClock systemClock;
    private final Duration lockTimeout;
    private final TaskRepository taskRepository;
    private volatile PersistentQueueObserver observer;

    public PersistentQueue(Database database, TaskRepository taskRepository, SystemClock systemClock, Duration lockTimeout) {
        this.database = database;
        this.systemClock = systemClock;
        this.lockTimeout = lockTimeout;
        this.taskRepository = taskRepository;
    }

    String add(Task task) {
        return taskRepository.add(task).getId();
    }

    String add(Connection connection, Task task) {
        return taskRepository.add(connection, task).getId();
    }

    List<Task> peekAndLock(Set<String> processableTasks, int limit) {
        return database.transaction(connection -> {
            List<Task> nextTasksToLock = findNextForExecution(connection, processableTasks, limit);

            if (nextTasksToLock.isEmpty()) {
                onPeek(limit, 0, 0);
                return Collections.emptyList();
            }

            if (Thread.currentThread().isInterrupted()) {
                return Collections.emptyList();
            }

            List<Task> locked = taskRepository.lock(connection, nextTasksToLock);

            int lockCount = locked.size();
            onPeek(limit, nextTasksToLock.size(), lockCount);
            if (lockCount > 0) {
                return locked;
            }

            return Collections.emptyList();
        });
    }

    private List<Task> findNextForExecution(Connection connection, Set<String> processableTasks, int limit) {
        return taskRepository.findAllNotLockedOrderedByCreatedDate(connection, processableTasks, plannedExecutionTime(), currentLockTimeout(), limit);
    }

    private void onPeek(int limit, int selected, int locked) {
        if (observer == null) {
            return;
        }

        observer.onPeek(limit, selected, locked);
    }

    private LocalDateTime plannedExecutionTime() {
        return systemClock.now();
    }

    private LocalDateTime currentLockTimeout() {
        return systemClock.now().minus(lockTimeout);
    }

    void setObserver(PersistentQueueObserver observer) {
        this.observer = observer;
    }

}
