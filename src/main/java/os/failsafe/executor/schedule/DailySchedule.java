package os.failsafe.executor.schedule;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

public class DailySchedule implements Schedule {

    private final LocalTime dailyTime;

    public DailySchedule(LocalTime dailyTime) {
        this.dailyTime = dailyTime;
    }

    @Override
    public Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime) {
        LocalDate nextExecutionDate;

        if(dailyTime.isAfter(currentTime.toLocalTime())) {
            nextExecutionDate = currentTime.toLocalDate();
        } else {
            nextExecutionDate = currentTime.toLocalDate().plusDays(1);
        }

        return Optional.of(LocalDateTime.of(nextExecutionDate, dailyTime));
    }
}
