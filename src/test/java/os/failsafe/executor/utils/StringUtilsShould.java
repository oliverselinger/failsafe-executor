package os.failsafe.executor.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringUtilsShould {

    @Test
    void abbreviate() {
        String abbreviatedText = StringUtils.abbreviate("failsafe-executor", 11);
        assertEquals("failsafe...", abbreviatedText);
    }
}
