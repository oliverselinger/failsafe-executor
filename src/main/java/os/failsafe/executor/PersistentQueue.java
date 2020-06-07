package os.failsafe.executor;

import os.failsafe.executor.task.PersistentTask;
import os.failsafe.executor.task.TaskId;
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

    TaskId add(TaskInstance task) {
        return persistentTaskRepository.add(task).getId();
    }

    TaskId add(Connection connection, TaskInstance task) {
        return persistentTaskRepository.add(connection, task).getId();
    }

    PersistentTask peekAndLock() {
        List<PersistentTask> nextTasksToLock = findNextForExecution();

        if (nextTasksToLock.isEmpty()) {
            return null;
        }

        do {
            Optional<PersistentTask> locked = nextTasksToLock.stream()
                    .map(persistentTaskRepository::lock)
                    .filter(Objects::nonNull)
                    .findFirst();

            if (locked.isPresent()) {
                return locked.get();
            }
        } while (!(nextTasksToLock = findNextForExecution()).isEmpty());

        return null;
    }

    private List<PersistentTask> findNextForExecution() {
        return persistentTaskRepository.findAllNotLockedOrderedByCreatedDate(plannedExecutionTime(), currentLockTimeout(), 3);
    }

    private LocalDateTime plannedExecutionTime() {
        return systemClock.now();
    }

    private LocalDateTime currentLockTimeout() {
        return systemClock.now().minus(lockTimeout);
    }

}
