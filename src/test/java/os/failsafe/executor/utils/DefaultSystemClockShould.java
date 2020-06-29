package os.failsafe.executor.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultSystemClockShould {

    private DefaultSystemClock defaultSystemClock;

    @BeforeEach
    void init() {
        defaultSystemClock = new DefaultSystemClock();
    }

    @Test
    void return_current_date_time() {
        LocalDateTime before = LocalDateTime.now();

        LocalDateTime now = defaultSystemClock.now();

        LocalDateTime after = LocalDateTime.now();

        assertNotNull(now);
        assertTrue(now.isEqual(before) || now.isAfter(before));
        assertTrue(now.isEqual(after) || now.isBefore(after));
    }
}
