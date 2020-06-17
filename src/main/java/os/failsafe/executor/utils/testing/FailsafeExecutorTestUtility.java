package os.failsafe.executor.utils.testing;

import os.failsafe.executor.FailsafeExecutor;
import os.failsafe.executor.Task;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FailsafeExecutorTestUtility {

    public static final long DEFAULT_TIMEOUT = 10;
    public static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.SECONDS;

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Runnable runnable, Consumer<List<Task>> failedTasksConsumer) {
        awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, DEFAULT_TIMEUNIT, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, long timeout, TimeUnit timeUnit, Runnable runnable, Consumer<List<Task>> failedTasksConsumer) {
        awaitAllTasks(failsafeExecutor, timeout, timeUnit, () -> {
            runnable.run();
            return Boolean.TRUE;
        }, failedTasksConsumer);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Supplier<T> supplier, Consumer<List<Task>> failedTasksConsumer) {
        return awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, DEFAULT_TIMEUNIT, supplier, failedTasksConsumer);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, long timeout, TimeUnit timeUnit, Supplier<T> supplier, Consumer<List<Task>> failedTasksConsumer) {
        AwaitableTaskExecutionListener taskExecutionListener = new AwaitableTaskExecutionListener(timeout, timeUnit);
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
