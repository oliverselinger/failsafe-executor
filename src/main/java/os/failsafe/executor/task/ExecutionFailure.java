package os.failsafe.executor.task;

import java.time.LocalDateTime;

public class ExecutionFailure {

    private final LocalDateTime failTime;
    private final String exceptionMessage;
    private final String stackTrace;

    public ExecutionFailure(LocalDateTime failTime, String exceptionMessage, String stackTrace) {
        this.failTime = failTime;
        this.exceptionMessage = exceptionMessage;
        this.stackTrace = stackTrace;
    }

    public LocalDateTime getFailTime() {
        return failTime;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public String getStackTrace() {
        return stackTrace;
    }
}
