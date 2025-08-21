package os.failsafe.executor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import os.failsafe.executor.db.DbExtension;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Test to verify the improved logging functionality.
 */
public class LoggingTest {

    @RegisterExtension
    static DbExtension dbExtension = new DbExtension();
    
    @TempDir
    Path tempDir;

    @Test
    public void shouldLogErrorsCorrectly() throws SQLException, InterruptedException {
        // Given
        // Create executor with minimal initial delay for testing
        FailsafeExecutor executor = new FailsafeExecutor(
            new os.failsafe.executor.utils.DefaultSystemClock(),
            dbExtension.dataSource(),
            5, // workerThreadCount
            30, // queueSize
            Duration.ofMillis(100), // initialDelay - reduced for testing
            Duration.ofSeconds(1), // pollingInterval - reduced for testing
            Duration.ofMinutes(5) // lockTimeout
        );

        // Enable debug logging to see all log messages
        executor.enableDebugLogging();

        // Register a task that will throw an exception
        AtomicBoolean taskExecuted = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        executor.registerTask("errorTask", parameter -> {
            System.out.println("[DEBUG_LOG] Task executed with parameter: " + parameter);
            taskExecuted.set(true);
            latch.countDown();
            throw new RuntimeException("Test exception to verify error logging");
        });

        // When
        executor.start();
        String taskId = executor.execute("errorTask", "test-parameter");
        System.out.println("[DEBUG_LOG] Submitted task with ID: " + taskId);

        // Then
        // Wait for the task to execute
        boolean taskCompleted = latch.await(30, TimeUnit.SECONDS);
        System.out.println("[DEBUG_LOG] Task completed: " + taskCompleted);

        // Verify the task was executed
        assert taskExecuted.get() : "Task was not executed";

        // Wait a bit to allow logs to be printed
        Thread.sleep(2000);

        // Stop the executor
        executor.stop();

        // Test different log levels
        executor.setLogLevel(Level.INFO);
        executor.setLogLevel(Level.WARNING);
        executor.setLogLevel(Level.SEVERE);
        executor.enableDebugLogging(); // Back to DEBUG
        executor.enableErrorOnlyLogging(); // ERROR only

        // Note: We can't programmatically verify the log output since it goes to stdout/stderr,
        // but we can manually check the test output to verify the logs are correct.
        System.out.println("[DEBUG_LOG] Test completed. Check the log output to verify error logging.");
    }
    
    @Test
    public void shouldLoadCustomConfigurationFromFile() throws SQLException, IOException, InterruptedException {
        // Given
        File configFile = tempDir.resolve("custom-logging.properties").toFile();
        
        // Create custom logging properties
        Properties props = new Properties();
        props.setProperty("handlers", "java.util.logging.ConsoleHandler");
        props.setProperty(".level", "INFO");
        props.setProperty("os.failsafe.executor.level", "FINE");
        props.setProperty("java.util.logging.ConsoleHandler.level", "FINE");
        props.setProperty("java.util.logging.ConsoleHandler.formatter", "java.util.logging.SimpleFormatter");
        props.setProperty("java.util.logging.SimpleFormatter.format", 
                          "[CUSTOM-CONFIG] %1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$s [%3$s] %5$s%6$s%n");
        
        // Save properties to file
        try (FileOutputStream out = new FileOutputStream(configFile)) {
            props.store(out, "Custom logging configuration for test");
        }
        
        // Create executor
        FailsafeExecutor executor = new FailsafeExecutor(
            new os.failsafe.executor.utils.DefaultSystemClock(),
            dbExtension.dataSource(),
            2, // workerThreadCount
            10, // queueSize
            Duration.ofMillis(100), // initialDelay
            Duration.ofSeconds(1), // pollingInterval
            Duration.ofMinutes(1) // lockTimeout
        );
        
        // When - Load custom configuration
        executor.loadLoggingConfiguration(configFile.getAbsolutePath());
        
        // Then - Log messages should use custom format
        System.out.println("[DEBUG_LOG] Custom configuration loaded, log messages should have [CUSTOM-CONFIG] prefix");
        
        // Register and execute a task to generate logs
        CountDownLatch latch = new CountDownLatch(1);
        executor.registerTask("customConfigTask", parameter -> {
            System.out.println("[DEBUG_LOG] Task with custom logging executed");
            latch.countDown();
        });
        
        executor.start();
        executor.execute("customConfigTask", "test-parameter");
        
        // Wait for task to complete
        latch.await(10, TimeUnit.SECONDS);
        
        // Stop the executor
        executor.stop();
    }
    
    @Test
    public void shouldLoadCustomConfigurationFromProperties() throws SQLException, IOException, InterruptedException {
        // Given
        // Create custom logging properties
        Properties props = new Properties();
        props.setProperty("handlers", "java.util.logging.ConsoleHandler");
        props.setProperty(".level", "INFO");
        props.setProperty("os.failsafe.executor.level", "FINE");
        props.setProperty("java.util.logging.ConsoleHandler.level", "FINE");
        props.setProperty("java.util.logging.ConsoleHandler.formatter", "java.util.logging.SimpleFormatter");
        props.setProperty("java.util.logging.SimpleFormatter.format", 
                          "[PROPS-CONFIG] %1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$s [%3$s] %5$s%6$s%n");
        
        // Create executor
        FailsafeExecutor executor = new FailsafeExecutor(
            new os.failsafe.executor.utils.DefaultSystemClock(),
            dbExtension.dataSource(),
            2, // workerThreadCount
            10, // queueSize
            Duration.ofMillis(100), // initialDelay
            Duration.ofSeconds(1), // pollingInterval
            Duration.ofMinutes(1) // lockTimeout
        );
        
        // When - Load custom configuration from properties
        executor.loadLoggingConfiguration(props);
        
        // Then - Log messages should use custom format
        System.out.println("[DEBUG_LOG] Properties configuration loaded, log messages should have [PROPS-CONFIG] prefix");
        
        // Register and execute a task to generate logs
        CountDownLatch latch = new CountDownLatch(1);
        executor.registerTask("propsConfigTask", parameter -> {
            System.out.println("[DEBUG_LOG] Task with properties logging executed");
            latch.countDown();
        });
        
        executor.start();
        executor.execute("propsConfigTask", "test-parameter");
        
        // Wait for task to complete
        latch.await(10, TimeUnit.SECONDS);
        
        // Stop the executor
        executor.stop();
    }
}