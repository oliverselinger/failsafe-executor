package os.failsafe.executor.utils;

import os.failsafe.executor.TaskExecutionListener;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class FailsafeExecutorMetricsCollector implements TaskExecutionListener {

    private final LongAdder persisted = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder finished = new LongAdder();
    private TimeUnit targetTimeUnit = TimeUnit.SECONDS;
    private long metricsStartTime = System.currentTimeMillis();

    public FailsafeExecutorMetricsCollector() {
    }

    /**
     * @param targetTimeUnit unit of time in which the rate is calculated
     */
    public FailsafeExecutorMetricsCollector(TimeUnit targetTimeUnit) {
        this.targetTimeUnit = targetTimeUnit;
    }

    public Result collect() {
        long now = System.currentTimeMillis();
        long persistedSum = persisted.sumThenReset();
        long failedSum = failed.sumThenReset();
        long finishedSum = finished.sumThenReset();

        Result result = new Result(metricsStartTime, now, persistedSum, failedSum, finishedSum, targetTimeUnit);

        metricsStartTime = now;

        return result;
    }

    @Override
    public void persisting(String name, String id, String parameter) {
        persisted.increment();
    }

    @Override
    public void retrying(String name, String id, String parameter) {
    }

    @Override
    public void succeeded(String name, String id, String parameter) {
        finished.increment();
    }

    @Override
    public void failed(String name, String id, String parameter, Exception exception) {
        finished.increment();
        failed.increment();
    }

    public static class Result {
        public final long persistedSum;
        public final long failedSum;
        public final long finishedSum;
        public final long metricsStartTimeInMillis;
        public final long metricsEndTimeInMillis;
        public final TimeUnit targetTimeUnit;
        public final double durationInTargetTimeUnit;
        public final double persistingRateInTargetTimeUnit;
        public final double failureRateInTargetTimeUnit;
        public final double finishingRateInTargetTimeUnit;

        private Result(long metricsStartTimeInMillis, long metricsEndTimeInMillis, long persistedSum, long failedSum, long finishedSum, TimeUnit targetTimeUnit) {
            this.persistedSum = persistedSum;
            this.failedSum = failedSum;
            this.finishedSum = finishedSum;
            this.metricsStartTimeInMillis = metricsStartTimeInMillis;
            this.metricsEndTimeInMillis = metricsEndTimeInMillis;
            this.targetTimeUnit = targetTimeUnit;
            this.durationInTargetTimeUnit = convertTimeUnit(metricsEndTimeInMillis - metricsStartTimeInMillis, TimeUnit.MILLISECONDS, targetTimeUnit);
            this.persistingRateInTargetTimeUnit = persistedSum / durationInTargetTimeUnit;
            this.failureRateInTargetTimeUnit = failedSum / durationInTargetTimeUnit;
            this.finishingRateInTargetTimeUnit = finishedSum / durationInTargetTimeUnit;
        }
    }

    private static double convertTimeUnit(double amount, TimeUnit from, TimeUnit to) {
        // is from or to the larger unit?
        if (from.ordinal() < to.ordinal()) { // from is smaller
            return amount / from.convert(1, to);
        } else {
            return amount * to.convert(1, from);
        }
    }
}
