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
package os.failsafe.executor.utils;

import os.failsafe.executor.task.Schedule;

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
