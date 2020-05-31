package os.failsafe.executor.task;

import java.util.function.Consumer;

public class Task {

    private final String name;
    private final Consumer<String> parameterConsumer;

    Task(String name, Runnable runnable) {
        this.name = name;
        this.parameterConsumer = ignore -> runnable.run();
    }

    Task(String name, Consumer<String> parameterConsumer) {
        this.name = name;
        this.parameterConsumer = parameterConsumer;
    }

    public String getName() {
        return name;
    }

    public void run(String optionalParameter) {
        parameterConsumer.accept(optionalParameter);
    }
}
