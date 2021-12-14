package os.failsafe.executor;

import os.failsafe.executor.utils.SystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatService {

    private final SystemClock systemClock;
    private final TaskRepository taskRepository;
    private final FailsafeExecutor failsafeExecutor;
    private final Duration heartbeatInterval;

    private final ConcurrentHashMap<String, Task> lockedTasks = new ConcurrentHashMap<>();

    public HeartbeatService(Duration heartbeatInterval, SystemClock systemClock, TaskRepository taskRepository, FailsafeExecutor failsafeExecutor) {
        this.heartbeatInterval = heartbeatInterval;
        this.systemClock = systemClock;
        this.taskRepository = taskRepository;
        this.failsafeExecutor = failsafeExecutor;
    }

    public void register(Task task) {
        lockedTasks.put(task.getId(), task);
    }

    public void unregister(Task task) {
        lockedTasks.remove(task.getId());
    }

    void heartbeat() {
        try {
            List<Task> toUpdate = findAllOutdatedLocks();
            if (toUpdate.isEmpty()) {
                return;
            }

            List<Task> updated = taskRepository.updateLockTime(toUpdate);

            for (Task task : updated) {
                lockedTasks.computeIfPresent(task.getId(), (k, v) -> task);
            }

            failsafeExecutor.clearException();

        } catch (Exception e) {
            failsafeExecutor.storeException(e);
        }
    }

    private List<Task> findAllOutdatedLocks() {
        List<Task> toUpdate = new ArrayList<>();
        LocalDateTime threshold = systemClock.now().minus(heartbeatInterval);

        for (String taskId : lockedTasks.keySet()) {
            Task task = lockedTasks.get(taskId);
            if (task != null && task.getLockTime().isBefore(threshold)) {
                toUpdate.add(task);
            }
        }
        return toUpdate;
    }
}
