package os.failsafe.executor.utils;

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
}
