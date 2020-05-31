package os.failsafe.executor.schedule;

import java.time.LocalDateTime;
import java.util.Optional;

public interface Schedule {

    Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime);
}
