/*******************************************************************************
 * MIT License
 *
 * Copyright (c) 2020 Oliver Selinger
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package os.failsafe.executor.schedule;

import org.junit.jupiter.api.Test;
import os.failsafe.executor.schedule.DailySchedule;

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
