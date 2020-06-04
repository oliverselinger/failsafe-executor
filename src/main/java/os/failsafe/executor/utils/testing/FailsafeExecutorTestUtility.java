package os.failsafe.executor.utils.testing;

import os.failsafe.executor.FailsafeExecutor;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class FailsafeExecutorTestUtility {

    public static final long DEFAULT_TIMEOUT = 10;
    public static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.SECONDS;

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, Runnable runnable) {
        awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, DEFAULT_TIMEUNIT, () -> {
            runnable.run();
            return Boolean.TRUE;
        });
    }

    public static void awaitAllTasks(FailsafeExecutor failsafeExecutor, long timeout, TimeUnit timeUnit, Runnable runnable) {
        awaitAllTasks(failsafeExecutor, timeout, timeUnit, () -> {
            runnable.run();
            return Boolean.TRUE;
        });
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, Supplier<T> supplier) {
        return awaitAllTasks(failsafeExecutor, DEFAULT_TIMEOUT, DEFAULT_TIMEUNIT, supplier);
    }

    public static <T> T awaitAllTasks(FailsafeExecutor failsafeExecutor, long timeout, TimeUnit timeUnit, Supplier<T> supplier) {
        AwaitableTaskExecutionListener taskExecutionListener = new AwaitableTaskExecutionListener(timeout, timeUnit);
        failsafeExecutor.subscribe(taskExecutionListener);

        T value = supplier.get();

        taskExecutionListener.awaitAllTasks();

        failsafeExecutor.unsubscribe(taskExecutionListener);

        return value;
    }

}
