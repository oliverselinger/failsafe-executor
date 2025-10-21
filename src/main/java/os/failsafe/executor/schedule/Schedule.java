package os.failsafe.executor.schedule;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

public interface Schedule {

    /**
     * With a {@link Schedule} you can either plan a one time execution in future or a recurring execution.
     *
     * <p>For a <b>one-time</b> execution just let this method return {@link Optional#empty()} after your planned execution time has past.</p>
     *
     * <p>A <b>recurring execution</b> requires this method to always return the next planned time for execution. For example see {@link DailySchedule}.</p>
     *
     * @param currentTime the current time after the task finished its execution successfully
     * @return the next planned execution time
     */
    Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime);
}
