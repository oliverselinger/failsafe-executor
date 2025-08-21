package os.failsafe.executor;

import os.failsafe.executor.utils.Log;
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
    private final Log logger = Log.get(HeartbeatService.class);

    private final ConcurrentHashMap<String, Task> lockedTasks = new ConcurrentHashMap<>();

    public HeartbeatService(Duration heartbeatInterval, SystemClock systemClock, TaskRepository taskRepository, FailsafeExecutor failsafeExecutor) {
        this.heartbeatInterval = heartbeatInterval;
        this.systemClock = systemClock;
        this.taskRepository = taskRepository;
        this.failsafeExecutor = failsafeExecutor;
    }

    public void register(Task task) {
        try {
            if (task == null) {
                logger.warn("Attempted to register null task");
                return;
            }
            
            String taskId = task.getId();
            if (taskId == null) {
                logger.warn("Attempted to register task with null ID");
                return;
            }
            
            lockedTasks.put(taskId, task);
            logger.debug("Registered task: " + task.getName() + " (ID: " + taskId + ")");
        } catch (Exception e) {
            logger.error("Error registering task", e);
        }
    }

    public void unregister(Task task) {
        try {
            if (task == null) {
                logger.warn("Attempted to unregister null task");
                return;
            }
            
            String taskId = task.getId();
            if (taskId == null) {
                logger.warn("Attempted to unregister task with null ID");
                return;
            }
            
            Task removed = lockedTasks.remove(taskId);
            if (removed != null) {
                logger.debug("Unregistered task: " + task.getName() + " (ID: " + taskId + ")");
            } else {
                logger.warn("Attempted to unregister task that was not registered: " + task.getName() + " (ID: " + taskId + ")");
            }
        } catch (Exception e) {
            logger.error("Error unregistering task", e);
        }
    }

    void heartbeat() {
        try {
            // If this method is called directly (not through the executor), 
            // it means the executor might have been terminated unexpectedly
            // The executeNextTasks method will handle the restart
            
            List<Task> toUpdate = findAllOutdatedLocks();
            if (toUpdate.isEmpty()) {
                logger.debug("No outdated locks to update");
                return;
            }

            logger.debug("Updating lock time for " + toUpdate.size() + " tasks");
            List<Task> updated = taskRepository.updateLockTime(toUpdate);

            if (updated.size() != toUpdate.size()) {
                logger.warn("Not all locks were updated. Expected: " + toUpdate.size() + ", Actual: " + updated.size());
            }

            for (Task task : updated) {
                lockedTasks.computeIfPresent(task.getId(), (k, v) -> task);
                logger.debug("Updated lock time for task: " + task.getName() + " (ID: " + task.getId() + ")");
            }

            failsafeExecutor.clearException();

        } catch (Exception e) {
            logger.error("Error during heartbeat operation", e);
            failsafeExecutor.storeException(e);
        }
    }

    private List<Task> findAllOutdatedLocks() {
        List<Task> toUpdate = new ArrayList<>();
        LocalDateTime threshold = systemClock.now().minus(heartbeatInterval);
        int totalLockedTasks = lockedTasks.size();
        
        logger.debug("Checking " + totalLockedTasks + " locked tasks for outdated locks (threshold: " + threshold + ")");
        
        for (String taskId : lockedTasks.keySet()) {
            try {
                Task task = lockedTasks.get(taskId);
                if (task == null) {
                    logger.warn("Task with ID " + taskId + " was found in keySet but returned null from get");
                    continue;
                }
                
                LocalDateTime lockTime = task.getLockTime();
                if (lockTime == null) {
                    logger.warn("Task with ID " + taskId + " has null lock time");
                    continue;
                }
                
                if (lockTime.isBefore(threshold)) {
                    logger.debug("Task " + task.getName() + " (ID: " + taskId + ") has outdated lock: " + lockTime);
                    toUpdate.add(task);
                }
            } catch (Exception e) {
                logger.error("Error checking lock for task ID: " + taskId, e);
                // Continue with other tasks even if one fails
            }
        }
        
        if (!toUpdate.isEmpty()) {
            logger.debug("Found " + toUpdate.size() + " tasks with outdated locks out of " + totalLockedTasks + " locked tasks");
        }
        
        return toUpdate;
    }
}
