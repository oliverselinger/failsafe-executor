package os.failsafe.executor;

import java.time.LocalDateTime;
import java.util.UUID;

class TaskInstance {

    public final String id;
    public final String name;
    public final String parameter;
    public final LocalDateTime plannedExecutionTime;

    TaskInstance(String name, String parameter, LocalDateTime plannedExecutionTime) {
        this(UUID.randomUUID().toString(), name, parameter, plannedExecutionTime);
    }

    TaskInstance(String id, String name, String parameter, LocalDateTime plannedExecutionTime) {
        this.id = id;
        this.name = name;
        this.parameter = parameter;
        this.plannedExecutionTime = plannedExecutionTime;
    }

}
