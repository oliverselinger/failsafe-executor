package os.failsafe.executor;

import os.failsafe.executor.utils.Log;
import os.failsafe.executor.utils.SystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class HeartbeatService {

    private final SystemClock systemClock;
    private final TaskRepository taskRepository;
    private final Duration heartbeatInterval;
    private final Log logger = Log.get(HeartbeatService.class);

    private final ConcurrentHashMap<String, Task> lockedTasks = new ConcurrentHashMap<>();

    public HeartbeatService(Duration heartbeatInterval, SystemClock systemClock, TaskRepository taskRepository) {
        this.heartbeatInterval = heartbeatInterval;
        this.systemClock = systemClock;
        this.taskRepository = taskRepository;
    }

    public void register(Task task) {
        lockedTasks.put(task.getId(), task);
        logger.debug("Registered task: " + task.getName() + " (ID: " + task.getId() + ")");
    }

    public void unregister(Task task) {
        Task removed = lockedTasks.remove(task.getId());
        if (removed != null) {
            logger.debug("Unregistered task: " + task.getName() + " (ID: " + task.getId() + ")");
        } else {
            logger.warn("Attempted to unregister task that was not registered: " + task.getName() + " (ID: " + task.getId() + ")");
        }
    }

    void heartbeat() {
        try {
            List<Task> toUpdate = findAllOutdatedLocks();

            logger.debug("Found " + toUpdate.size() + " tasks with outdated locks out of " + lockedTasks.size() + " locked tasks");

            if (toUpdate.isEmpty()) {
                return;
            }

            taskRepository.updateLockTime(toUpdate).forEach(task -> {
                lockedTasks.computeIfPresent(task.getId(), (k, v) -> task);
                logger.debug("Updated lock time for task: " + task.getName() + " (ID: " + task.getId() + ") with lockTime: " + task.getLockTime());
            });

        } catch (Exception e) {
            logger.error("Error during heartbeat operation", e);
        }
    }

    private List<Task> findAllOutdatedLocks() {
        LocalDateTime threshold = systemClock.now().minus(heartbeatInterval);
        return lockedTasks.values().stream()
                .filter(task -> task != null && task.getLockTime() != null && task.getLockTime().isBefore(threshold))
                .collect(Collectors.toList());
    }
}
