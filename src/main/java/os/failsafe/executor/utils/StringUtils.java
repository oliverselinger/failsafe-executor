package os.failsafe.executor.utils;

import java.io.IOException;
import java.io.Reader;

public class StringUtils {

    private StringUtils() {
    }

    public static boolean isBlank(String str) {
        return str == null || str.isEmpty();
    }

    public static String abbreviate(String input, int maxLength) {
        if (input == null) {
            return null;
        }

        if (input.length() <= maxLength)
            return input;
        else
            return input.substring(0, maxLength-3) + "...";
    }

    public static String fromReader(Reader reader) throws IOException {
        char[] buffer = new char[4096];
        StringBuilder builder = new StringBuilder();
        int numChars;

        while ((numChars = reader.read(buffer)) >= 0) {
            builder.append(buffer, 0, numChars);
        }

        return builder.toString();
    }
}
