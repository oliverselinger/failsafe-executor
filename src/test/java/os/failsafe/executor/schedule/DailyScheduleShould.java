package os.failsafe.executor.schedule;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DailyScheduleShould {

    private final LocalTime dailyTime = LocalTime.of(22, 0);
    private final DailySchedule dailySchedule = new DailySchedule(dailyTime);

    private final LocalDate currentDay = LocalDate.now();
    private final LocalDateTime beforeDailyTime = LocalDateTime.of(currentDay, dailyTime.minusMinutes(1));
    private final LocalDateTime afterDailyTime = LocalDateTime.of(currentDay, dailyTime.plusMinutes(1));

    @Test void
    return_current_day_if_daily_time_is_on_same_day_in_future() {
        LocalDateTime nextExecutionTime = dailySchedule.nextExecutionTime(beforeDailyTime).get();

        assertEquals(currentDay, nextExecutionTime.toLocalDate());
        assertEquals(dailyTime, nextExecutionTime.toLocalTime());
    }

    @Test void
    return_next_day_if_daily_time_is_already_past_on_same_day() {
        LocalDateTime nextExecutionTime = dailySchedule.nextExecutionTime(afterDailyTime).get();

        assertEquals(currentDay.plusDays(1), nextExecutionTime.toLocalDate());
        assertEquals(dailyTime, nextExecutionTime.toLocalTime());
    }
}
