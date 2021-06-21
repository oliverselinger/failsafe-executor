package os.failsafe.executor.utils.testing;

import os.failsafe.executor.TaskExecutionListener;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwaitableTaskExecutionListener implements TaskExecutionListener {

    public static final NoWaitPredicate WAIT_FOR_ALL_TASKS = (name, taskId, parameter) -> false;
    private final Duration timeout;
    private final NoWaitPredicate taskFilter;
    private final ConcurrentHashMap<String, String> taskMap = new ConcurrentHashMap<>();
    private final Phaser phaser = new Phaser(1);
    private final List<String> failedTasksByIds = new CopyOnWriteArrayList<>();

    public AwaitableTaskExecutionListener(Duration timeout) {
        this.timeout = timeout;
        this.taskFilter = WAIT_FOR_ALL_TASKS;
    }

    public AwaitableTaskExecutionListener(Duration timeout, NoWaitPredicate taskFilter) {
        this.timeout = timeout;
        this.taskFilter = taskFilter;
    }

    @Override
    public void persisting(String name, String taskId, String parameter, boolean registered) {
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
        if (taskFilter.shouldNotWaitForTask(name, taskId, parameter)) {
            return;
        }

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
        phaser.arrive();

        try {
            phaser.awaitAdvanceInterruptibly(0, timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Only " + (phaser.getArrivedParties()-1) + "/" + (phaser.getRegisteredParties()-1) + " tasks finished! Waiting for: " + taskMap.toString());
        }
    }

    public boolean isAnyExecutionFailed() {
        return !failedTasksByIds.isEmpty();
    }

    public List<String> failedTasksByIds() {
        return failedTasksByIds;
    }

}
