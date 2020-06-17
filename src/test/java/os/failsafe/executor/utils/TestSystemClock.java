package os.failsafe.executor.utils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class TestSystemClock implements SystemClock {

    private Clock clock = Clock.systemDefaultZone();

    @Override
    public LocalDateTime now() {
        return LocalDateTime.now(clock).truncatedTo(ChronoUnit.MILLIS);
    }

    public void timeTravelBy(Duration duration) {
        this.clock = Clock.offset(this.clock, duration);
    }

    public void fixedTime(LocalDateTime now) {
        Instant instant = now.atZone(ZoneId.systemDefault()).toInstant();
        this.clock = Clock.fixed(instant, ZoneId.systemDefault());
    }

    public void resetTime() {
        this.clock = Clock.systemDefaultZone();
    }
}
