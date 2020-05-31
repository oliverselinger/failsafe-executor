package os.failsafe.executor;

import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

class PersistentQueue {

    private static final String QUERY_ALL_QUEUED_TASKS = "SELECT * FROM FAILSAFE_TASK WHERE FAILED = 0 AND (LOCK_TIME IS NULL OR (LOCK_TIME <= ?))";
    private static final String QUERY_ORDER_BY_CREATED_DATE = " ORDER BY CREATED_DATE";
    private static final String QUERY_PLANNED_EXECUTION_TIME = " AND PLANNED_EXECUTION_TIME <= ?";
    private static final String QUERY_LIMIT = " FETCH FIRST 3 ROWS ONLY";
    private static final String QUERY_LIMIT_MYSQL = " LIMIT 3";

    private static final String QUERY_ALL_TASKS_ORDERED = QUERY_ALL_QUEUED_TASKS + QUERY_ORDER_BY_CREATED_DATE;
    private final String QUERY_NEXT_TASKS;

    private final Database database;
    private final SystemClock systemClock;
    private final Duration lockTimeout;
    private final PersistentTasks persistentTasks;

    public PersistentQueue(Database database, SystemClock systemClock, Duration lockTimeout) {
        this.database = database;
        this.systemClock = systemClock;
        this.lockTimeout = lockTimeout;
        this.persistentTasks = new PersistentTasks(database, systemClock);

        this.QUERY_NEXT_TASKS = constructNextTaskQuery();
    }

    TaskId add(TaskInstance task) {
        return persistentTasks.create(task).getId();
    }

    TaskId add(Connection connection, TaskInstance task) {
        return persistentTasks.create(connection, task).getId();
    }

    List<PersistentTask> allQueued() {
        return database.selectAll(QUERY_ALL_TASKS_ORDERED, persistentTasks::mapToPersistentTask, deadExecutionTimeout());
    }

    PersistentTask peekAndLock() {
        return database.connect(connection -> {

            List<PersistentTask> nextTasksToLock = findNextTasks(connection);

            if (nextTasksToLock.isEmpty()) {
                return null;
            }

            do {
                Optional<PersistentTask> locked = nextTasksToLock.stream()
                        .map(enqueuedTask -> enqueuedTask.lock(connection))
                        .findFirst();

                if (locked.isPresent()) {
                    return locked.get();
                }
            } while((nextTasksToLock = findNextTasks(connection)).isEmpty());

            return null;
        });
    }

    private List<PersistentTask> findNextTasks(Connection connection) {
        return database.selectAll(connection, QUERY_NEXT_TASKS, persistentTasks::mapToPersistentTask, deadExecutionTimeout(), expectedPlannedExecutionTime());
    }

    private Timestamp deadExecutionTimeout() {
        return Timestamp.valueOf(systemClock.now().minus(lockTimeout));
    }

    private Timestamp expectedPlannedExecutionTime() {
        return Timestamp.valueOf(systemClock.now());
    }

    private String constructNextTaskQuery() {
        return QUERY_ALL_QUEUED_TASKS + QUERY_PLANNED_EXECUTION_TIME + QUERY_ORDER_BY_CREATED_DATE + (database.isMysql() ? QUERY_LIMIT_MYSQL : QUERY_LIMIT);
    }

}
