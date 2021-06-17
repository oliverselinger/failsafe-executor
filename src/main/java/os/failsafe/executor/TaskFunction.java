package os.failsafe.executor;

/**
 * This is a functional interface which represents a lambda, accepting a parameter.
 */
public interface TaskFunction<T> {
    void accept(T var1) throws Exception;
}
