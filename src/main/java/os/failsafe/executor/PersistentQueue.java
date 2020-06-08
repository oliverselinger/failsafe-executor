package os.failsafe.executor;

import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

class PersistentQueue {

    private final SystemClock systemClock;
    private final Duration lockTimeout;
    private final PersistentTaskRepository persistentTaskRepository;

    public PersistentQueue(PersistentTaskRepository persistentTaskRepository, SystemClock systemClock, Duration lockTimeout) {
        this.systemClock = systemClock;
        this.lockTimeout = lockTimeout;
        this.persistentTaskRepository = persistentTaskRepository;
    }

    String add(Task task) {
        return persistentTaskRepository.add(task).getId();
    }

    String add(Connection connection, Task task) {
        return persistentTaskRepository.add(connection, task).getId();
    }

    Task peekAndLock() {
        List<Task> nextTasksToLock = findNextForExecution();

        if (nextTasksToLock.isEmpty()) {
            return null;
        }

        do {
            Optional<Task> locked = nextTasksToLock.stream()
                    .map(persistentTaskRepository::lock)
                    .filter(Objects::nonNull)
                    .findFirst();

            if (locked.isPresent()) {
                return locked.get();
            }
        } while (!(nextTasksToLock = findNextForExecution()).isEmpty());

        return null;
    }

    private List<Task> findNextForExecution() {
        return persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(plannedExecutionTime(), currentLockTimeout(), 3);
    }

    private LocalDateTime plannedExecutionTime() {
        return systemClock.now();
    }

    private LocalDateTime currentLockTimeout() {
        return systemClock.now().minus(lockTimeout);
    }

}
