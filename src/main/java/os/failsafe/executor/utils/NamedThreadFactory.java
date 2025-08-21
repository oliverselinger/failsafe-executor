package os.failsafe.executor.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class NamedThreadFactory implements ThreadFactory {

    private final AtomicLong threadIndex = new AtomicLong(0);
    private final String threadNamePrefix;
    private final Log logger = Log.get(NamedThreadFactory.class);

    public NamedThreadFactory(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName(threadNamePrefix + threadIndex.getAndIncrement());
        
        // Set an uncaught exception handler to prevent the executor from silently dying
        thread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("Uncaught exception in thread " + t.getName() + ". This could cause the executor to stop processing tasks.", e);
            // The thread will still die, but at least we log the error
        });
        
        return thread;
    }

}