package os.failsafe.executor.utils.testing;

import os.failsafe.executor.TaskExecutionListener;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwaitableTaskExecutionListener implements TaskExecutionListener {

    private final long timeout;
    private final TimeUnit timeUnit;
    private final ConcurrentHashMap<String, String> taskMap = new ConcurrentHashMap<>();
    private final Phaser phaser = new Phaser();

    public AwaitableTaskExecutionListener(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public void registered(String name, String taskId, String parameter) {
        taskMap.computeIfAbsent(taskId, key -> {
            phaser.register();
            return taskId;
        });
    }

    @Override
    public void succeeded(String name, String taskId, String parameter) {
        arrive(taskId);
    }

    @Override
    public void failed(String name, String taskId, String parameter) {
        arrive(taskId);
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
}
