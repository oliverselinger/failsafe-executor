package os.failsafe.executor.utils.testing;

import os.failsafe.executor.FailsafeExecutor;
import os.failsafe.executor.Task;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FailsafeExecutorTestUtility {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Runnable runnable, Consumer<List<Task>> failedTasksConsumer) {
        awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Runnable runnable, Consumer<List<Task>> failedTasksConsumer) {
        awaitAllTasks(failsafeExecutor, timeout, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Runnable runnable, Consumer<List<Task>> failedTasksConsumer, NoWaitPredicate taskFilter) {
        awaitAllTasks(failsafeExecutor, timeout, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer, taskFilter);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Supplier<T> supplier, Consumer<List<Task>> failedTasksConsumer) {
        return awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, supplier, failedTasksConsumer);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Supplier<T> supplier, Consumer<List<Task>> failedTasksConsumer) {
        return awaitAllTasks(failsafeExecutor, timeout, supplier, failedTasksConsumer, AwaitableTaskExecutionListener.WAIT_FOR_ALL_TASKS);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Supplier<T> supplier, Consumer<List<Task>> failedTasksConsumer, NoWaitPredicate taskFilter) {
        AwaitableTaskExecutionListener taskExecutionListener = new AwaitableTaskExecutionListener(timeout, taskFilter);
        failsafeExecutor.subscribe(taskExecutionListener);

        T value = supplier.get();

        taskExecutionListener.awaitAllTasks();

        failsafeExecutor.unsubscribe(taskExecutionListener);

        if (taskExecutionListener.isAnyExecutionFailed()) {
            List<Task> failedTasks = taskExecutionListener.failedTasksByIds().stream()
                    .map(failsafeExecutor::task)
                    .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                    .collect(Collectors.toList());

            failedTasksConsumer.accept(failedTasks);
        }

        return value;
    }

}
