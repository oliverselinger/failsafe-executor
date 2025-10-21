package os.failsafe.executor.utils;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.InputStream;
import java.util.logging.LogManager;

public class LoggingTestSetup implements BeforeAllCallback {

    private static boolean initialized = false;

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!initialized) {
            initialized = true;

            try (InputStream is = getClass().getResourceAsStream("/logging.properties")) {
                if (is != null) {
                    LogManager.getLogManager().readConfiguration(is);
                    System.out.println("✅ java.util.logging configured from classpath logging.properties");
                } else {
                    System.err.println("⚠️ logging.properties not found on classpath");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
