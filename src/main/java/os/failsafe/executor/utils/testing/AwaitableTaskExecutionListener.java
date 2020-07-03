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
    public void persisting(String name, String taskId, String parameter) {
        register(name, taskId, parameter);
    }

    @Override
    public void retrying(String name, String taskId, String parameter) {
        register(name, taskId, parameter);
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

    private void register(String name, String taskId, String parameter) {
        taskMap.computeIfAbsent(taskId, key -> {
            phaser.register();
            return name + "#" + parameter;
        });
    }

    private void arrive(String taskId) {
        if (taskMap.remove(taskId) != null) {
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
            throw new RuntimeException("Only " + phaser.getArrivedParties() + "/" + phaser.getRegisteredParties() + " tasks finished! Waiting for: " + taskMap.toString());
        }
    }

    public boolean isAnyExecutionFailed() {
        return !failedTasksByIds.isEmpty();
    }

    public List<String> failedTasksByIds() {
        return failedTasksByIds;
    }

}
