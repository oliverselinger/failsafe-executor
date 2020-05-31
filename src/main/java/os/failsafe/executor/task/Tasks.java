package os.failsafe.executor.task;

import java.util.function.Consumer;

public class Tasks {

    public static Task runnable(String name, Runnable runnable) {
        return new Task(name, runnable);
    }

    public static Task parameterized(String name, Consumer<String> parameterConsumer) {
        return new Task(name, parameterConsumer);
    }

}
