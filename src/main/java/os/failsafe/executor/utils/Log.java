package os.failsafe.executor.utils;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Log {

    private final Logger logger;

    private Log(Class<?> clazz) {
        this.logger = Logger.getLogger(clazz.getName());
    }

    public static Log get(Class<?> clazz) {
        return new Log(clazz);
    }

    public void trace(String msg) {
        logger.finer(msg);
    }

    public void debug(String msg) {
        logger.fine(msg);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void warn(String msg) {
        logger.warning(msg);
    }

    public void error(String msg) {
        logger.severe(msg);
    }

    public void error(String msg, Throwable t) {
        logger.log(Level.SEVERE, msg, t);
    }

    public static void setLevel(Level level) {
        // Get the package logger
        java.util.logging.Logger pkgLogger = java.util.logging.Logger.getLogger("os.failsafe.executor");
        pkgLogger.setLevel(level);

        // Also ensure handlers are at least at this level
        for (Handler handler : pkgLogger.getHandlers()) {
            handler.setLevel(level);
        }
    }
}