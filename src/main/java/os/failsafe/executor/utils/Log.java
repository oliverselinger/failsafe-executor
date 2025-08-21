package os.failsafe.executor.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * A simple logger for the failsafe-executor using Java's built-in logging.
 * Provides basic logging functionality with different log levels.
 * 
 * The logger uses a default configuration from the classpath resource 'logging.properties'.
 * This configuration can be overridden by:
 * 1. Calling {@link #loadConfiguration(String)} with a path to a custom properties file
 * 2. Setting the system property 'java.util.logging.config.file' to a custom properties file
 */
public final class Log {

    private static final String DEFAULT_CONFIG = "/logging.properties";
    private static boolean configurationLoaded = false;
    
    private final Logger logger;

    static {
        // Load default configuration if not already loaded
        loadDefaultConfiguration();
    }
    
    private Log(Class<?> clazz) {
        this.logger = Logger.getLogger(clazz.getName());
    }

    // Factory method like SLF4J
    public static Log get(Class<?> clazz) {
        return new Log(clazz);
    }
    
    /**
     * Loads the default logging configuration from the classpath.
     * This is automatically called when the Log class is loaded.
     */
    private static synchronized void loadDefaultConfiguration() {
        if (configurationLoaded) {
            return;
        }
        
        // Skip if system property is set (user has provided their own config)
        if (System.getProperty("java.util.logging.config.file") != null) {
            configurationLoaded = true;
            return;
        }
        
        try (InputStream is = Log.class.getResourceAsStream(DEFAULT_CONFIG)) {
            if (is != null) {
                LogManager.getLogManager().readConfiguration(is);
                configurationLoaded = true;
            }
        } catch (IOException e) {
            System.err.println("Failed to load default logging configuration: " + e.getMessage());
        }
    }
    
    /**
     * Loads a custom logging configuration from the specified file path.
     * This will override the default configuration.
     *
     * @param configFilePath Path to the logging properties file
     * @throws IOException If the file cannot be read
     */
    public static synchronized void loadConfiguration(String configFilePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(configFilePath)) {
            LogManager.getLogManager().readConfiguration(fis);
            configurationLoaded = true;
        }
    }
    
    /**
     * Loads a custom logging configuration from the provided properties.
     * This will override the default configuration.
     *
     * @param properties Logging properties
     * @throws IOException If the properties cannot be loaded
     */
    public static synchronized void loadConfiguration(Properties properties) throws IOException {
        LogManager.getLogManager().readConfiguration(
            properties.entrySet().stream()
                .reduce(new Properties(), 
                    (p, e) -> {
                        p.put(e.getKey(), e.getValue());
                        return p;
                    }, 
                    (p1, p2) -> {
                        p1.putAll(p2);
                        return p1;
                    })
                .entrySet().stream()
                .reduce(
                    new java.io.ByteArrayInputStream(new byte[0]),
                    (is, e) -> new java.io.ByteArrayInputStream((e.getKey() + "=" + e.getValue() + "\n").getBytes()),
                    (is1, is2) -> is1
                )
        );
        configurationLoaded = true;
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

    public void debug(String msg) {
        // Java util logging does not have a debug level by default
        logger.fine(msg);
    }
    
    public void debug(String msg, Throwable t) {
        logger.log(Level.FINE, msg, t);
    }
    
    public void warn(String msg, Throwable t) {
        logger.log(Level.WARNING, msg, t);
    }
    
    /**
     * Sets the log level on package level.
     *
     * @param level The minimum level to log
     */
    public static void setLevel(Level level) {
        // Get the package logger
        java.util.logging.Logger pkgLogger = java.util.logging.Logger.getLogger("os.failsafe.executor");
        pkgLogger.setLevel(level);

        // Also ensure handlers are at least at this level
        for (Handler handler : pkgLogger.getHandlers()) {
            handler.setLevel(level);
        }
    }
    
    /**
     * Enables debug logging.
     */
    public static void enableDebugLogging() {
        setLevel(Level.FINE);
    }
    
    /**
     * Enables error-only logging.
     */
    public static void enableErrorOnlyLogging() {
        setLevel(Level.SEVERE);
    }
}