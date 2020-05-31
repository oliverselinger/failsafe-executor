package os.failsafe.executor.utils;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {

    private ExceptionUtils() {
    }

    public static String stackTraceAsString(Exception exception) {
        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

}
