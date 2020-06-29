package os.failsafe.executor;

import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

class PersistentQueue {

    private final SystemClock systemClock;
    private final Duration lockTimeout;
    private final TaskRepository taskRepository;

    public PersistentQueue(TaskRepository taskRepository, SystemClock systemClock, Duration lockTimeout) {
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

    Task peekAndLock(Set<String> processableTasks) {
        List<Task> nextTasksToLock = findNextForExecution(processableTasks);

        if (nextTasksToLock.isEmpty()) {
            return null;
        }

        if (Thread.currentThread().isInterrupted()) {
            return null;
        }

        do {
            Optional<Task> locked = nextTasksToLock.stream()
                    .map(taskRepository::lock)
                    .filter(Objects::nonNull)
                    .findFirst();

            if (locked.isPresent()) {
                return locked.get();
            }

            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        } while (!(nextTasksToLock = findNextForExecution(processableTasks)).isEmpty());

        return null;
    }

    private List<Task> findNextForExecution(Set<String> processableTasks) {
        return taskRepository.findAllNotLockedOrderedByCreatedDate(processableTasks, plannedExecutionTime(), currentLockTimeout(), 3);
    }

    private LocalDateTime plannedExecutionTime() {
        return systemClock.now();
    }

    private LocalDateTime currentLockTimeout() {
        return systemClock.now().minus(lockTimeout);
    }

}
