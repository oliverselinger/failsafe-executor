package os.failsafe.executor.utils;

public interface Throwing {
    interface Runnable {
        void run() throws Exception;
    }

    interface Supplier<T> {
        T get() throws Exception;
    }

    interface Consumer<T> {
        void accept(T var1) throws Exception;
    }
}
