package os.failsafe.executor.utils.testing;

import os.failsafe.executor.TaskExecutionListener;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwaitableTaskExecutionListener implements TaskExecutionListener {

    private final long timeout;
    private final TimeUnit timeUnit;
    private final ConcurrentHashMap<String, String> taskMap = new ConcurrentHashMap<>();
    private final Phaser phaser = new Phaser();
    private final List<String> failedTasksByIds = new CopyOnWriteArrayList<>();

    public AwaitableTaskExecutionListener(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public void persisted(String name, String taskId, String parameter) {
        register(taskId);
    }

    @Override
    public void retrying(String name, String taskId, String parameter) {
        register(taskId);
    }

    @Override
    public void succeeded(String name, String taskId, String parameter) {
        arrive(taskId);
    }

    @Override
    public void failed(String name, String taskId, String parameter, Exception exception) {
        failedTasksByIds.add(taskId);
        arrive(taskId);
    }

    private void register(String taskId) {
        taskMap.computeIfAbsent(taskId, key -> {
            phaser.register();
            return taskId;
        });
    }

    private void arrive(String taskId) {
        if (taskMap.containsKey(taskId)) {
            phaser.arrive();
        }
    }

    public void awaitAllTasks() {
        if (phaser.getRegisteredParties() == 0) {
            return;
        }

        try {
            phaser.awaitAdvanceInterruptibly(0, timeout, timeUnit);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Only " + phaser.getArrivedParties() + "/" + phaser.getRegisteredParties() + " tasks finished");
        }
    }

    public boolean isAnyExecutionFailed() {
        return !failedTasksByIds.isEmpty();
    }

    public List<String> failedTasksByIds() {
        return failedTasksByIds;
    }

}
