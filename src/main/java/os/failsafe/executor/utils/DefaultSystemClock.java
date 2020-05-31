package os.failsafe.executor.utils;

import java.time.LocalDateTime;

public class DefaultSystemClock implements SystemClock {

    @Override
    public LocalDateTime now() {
        return LocalDateTime.now();
    }
}
