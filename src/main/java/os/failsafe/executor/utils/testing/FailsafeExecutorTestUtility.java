package os.failsafe.executor.utils.testing;

import os.failsafe.executor.FailsafeExecutor;
import os.failsafe.executor.Task;
import os.failsafe.executor.utils.Throwing;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FailsafeExecutorTestUtility {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Throwing.Runnable runnable, Throwing.Consumer<List<Task>> failedTasksConsumer) throws Exception {
        awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Throwing.Runnable runnable, Throwing.Consumer<List<Task>> failedTasksConsumer) throws Exception {
        awaitAllTasks(failsafeExecutor, timeout, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Throwing.Runnable runnable, Throwing.Consumer<List<Task>> failedTasksConsumer, NoWaitPredicate taskFilter) throws Exception {
        awaitAllTasks(failsafeExecutor, timeout, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer, taskFilter);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Throwing.Supplier<T> supplier, Throwing.Consumer<List<Task>> failedTasksConsumer) throws Exception {
        return awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, supplier, failedTasksConsumer);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Throwing.Supplier<T> supplier, Throwing.Consumer<List<Task>> failedTasksConsumer) throws Exception {
        return awaitAllTasks(failsafeExecutor, timeout, supplier, failedTasksConsumer, AwaitableTaskExecutionListener.WAIT_FOR_ALL_TASKS);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Duration timeout, Throwing.Supplier<T> supplier, Throwing.Consumer<List<Task>> failedTasksConsumer, NoWaitPredicate taskFilter) throws Exception {
        AwaitableTaskExecutionListener taskExecutionListener = new AwaitableTaskExecutionListener(timeout, taskFilter);
        failsafeExecutor.subscribe(taskExecutionListener);

        T value = supplier.get();

        taskExecutionListener.awaitAllTasks();

        failsafeExecutor.unsubscribe(taskExecutionListener);

        if (taskExecutionListener.isAnyExecutionFailed()) {
            List<Task> failedTasks = taskExecutionListener.failedTasksByIds().stream()
                    .map(failsafeExecutor::findOne)
                    .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                    .collect(Collectors.toList());

            failedTasksConsumer.accept(failedTasks);
        }

        return value;
    }

}
