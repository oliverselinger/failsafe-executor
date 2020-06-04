package os.failsafe.executor.utils.testing;

import os.failsafe.executor.task.TaskExecutionListener;
import os.failsafe.executor.task.TaskId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AwaitableTaskExecutionListener implements TaskExecutionListener {

    private final long timeout;
    private final TimeUnit timeUnit;
    private final ConcurrentHashMap<TaskId, TaskId> taskMap = new ConcurrentHashMap<>();
    private final Phaser phaser = new Phaser();

    public AwaitableTaskExecutionListener(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public void registered(String s, TaskId taskId, String s1) {
        taskMap.computeIfAbsent(taskId, key -> {
            phaser.register();
            return taskId;
        });
    }

    @Override
    public void succeeded(String s, TaskId taskId, String s1) {
        arrive(taskId);
    }

    @Override
    public void failed(String s, TaskId taskId, String s1) {
        arrive(taskId);
    }

    private void arrive(TaskId taskId) {
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
            throw new RuntimeException(e);
        }
    }
}
