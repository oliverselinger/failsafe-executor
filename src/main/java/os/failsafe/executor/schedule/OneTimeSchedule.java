package os.failsafe.executor.schedule;

import java.time.LocalDateTime;
import java.util.Optional;

public class OneTimeSchedule implements Schedule {
    @Override
    public Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime) {
        return Optional.empty();
    }
}
