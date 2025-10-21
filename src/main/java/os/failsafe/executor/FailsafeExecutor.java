package os.failsafe.executor;

import os.failsafe.executor.schedule.Schedule;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.DefaultSystemClock;
import os.failsafe.executor.utils.Log;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;
import os.failsafe.executor.utils.Throwing;
import os.failsafe.executor.utils.Transaction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class FailsafeExecutor {

    public static final int DEFAULT_WORKER_THREAD_COUNT = 5;
    public static final int DEFAULT_QUEUE_SIZE = DEFAULT_WORKER_THREAD_COUNT * 6;
    public static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(10);
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5);
    public static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofMinutes(5);
    public static final String DEFAULT_TABLE_NAME = "FAILSAFE_TASK";
    public static final int NODE_ID_MAX_LENGTH = 48;


    private final Map<String, TaskRegistration> taskRegistrationsByName = new ConcurrentHashMap<>();
    private final Set<String> taskNamesWithFunctions = new CopyOnWriteArraySet<>();
    private final List<TaskExecutionListener> listeners = new CopyOnWriteArrayList<>();

    private ScheduledExecutorService executor;
    private final PersistentQueue persistentQueue;
    private final WorkerPool workerPool;
    private final TaskRepository taskRepository;
    private final Duration initialDelay;
    private final Duration pollingInterval;
    private final Database database;
    private final SystemClock systemClock;
    private final HeartbeatService heartbeatService;
    private final Duration heartbeatInterval;
    private final int queueSize;

    private volatile Exception lastRunException;
    private volatile String nodeId;

    private final Log logger = Log.get(FailsafeExecutor.class);

    public FailsafeExecutor(DataSource dataSource) throws SQLException {
        this(new DefaultSystemClock(), dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, DEFAULT_INITIAL_DELAY, DEFAULT_POLLING_INTERVAL, DEFAULT_LOCK_TIMEOUT);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout) throws SQLException {
        this(systemClock, dataSource, workerThreadCount, queueSize, initialDelay, pollingInterval, lockTimeout, DEFAULT_TABLE_NAME);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout, String tableName) throws SQLException {
        logger.info("Initializing FailsafeExecutor with parameters:");
        logger.info("  - Worker thread count: " + workerThreadCount);
        logger.info("  - Queue size: " + queueSize);
        logger.info("  - Initial delay: " + initialDelay);
        logger.info("  - Polling interval: " + pollingInterval);
        logger.info("  - Lock timeout: " + lockTimeout);
        logger.info("  - Table name: " + tableName);

        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
        this.queueSize = queueSize;
        this.heartbeatInterval = Duration.ofMillis(lockTimeout.toMillis() / 4);

        logger.info("  - Heartbeat interval: " + heartbeatInterval);

        if (queueSize < workerThreadCount) {
            logger.error("QueueSize must be >= workerThreadCount");
            throw new IllegalArgumentException("QueueSize must be >= workerThreadCount");
        }

        if (lockTimeout.compareTo(Duration.ofMinutes(1)) < 0) {
            logger.warn("LockTimeout very short! Recommendation is >= 1 minute");
        }

        try {
            this.database = new Database(dataSource);

            this.systemClock = () -> systemClock.now().truncatedTo(ChronoUnit.MILLIS);

            this.taskRepository = new TaskRepository(database, tableName, systemClock);

            this.persistentQueue = new PersistentQueue(database, taskRepository, systemClock, lockTimeout);

            this.heartbeatService = new HeartbeatService(heartbeatInterval, systemClock, taskRepository);

            this.workerPool = new WorkerPool(workerThreadCount, queueSize, heartbeatService);

            validateDatabase(dataSource);
        } catch (Exception e) {
            logger.error("Failed to initialize FailsafeExecutor", e);
            throw e;
        }
    }

    /**
     * Start execution of any submitted tasks.
     */
    public void start() {
        start(null);
    }

    /**
     * Start execution of any submitted tasks.
     * @param nodeId id of the node that locks the given task
     */
    public void start(String nodeId) {
        try {
            if (nodeId != null && nodeId.length() > NODE_ID_MAX_LENGTH) {
                String errorMsg = String.format("Length of nodeId can not exceed %d", NODE_ID_MAX_LENGTH);
                logger.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }

            if (executor != null && !executor.isShutdown()) {
                logger.warn("FailsafeExecutor is running, ignoring start request");
                return;
            }

            this.nodeId = nodeId;

            this.executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));

            executor.scheduleWithFixedDelay(
                    this::executeNextTasks,
                    initialDelay.toMillis(), pollingInterval.toMillis(), TimeUnit.MILLISECONDS);

            workerPool.start();

            executor.scheduleWithFixedDelay(heartbeatService::heartbeat, initialDelay.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);

            logger.info("Started FailsafeExecutor" + (nodeId != null ? " with nodeId: " + nodeId : ""));
        } catch (Exception e) {
            logger.error("Failed to start FailsafeExecutor", e);
            throw e;
        }
    }

    /**
     * Initiates an orderly shutdown in which previously locked tasks are executed,
     * but no new tasks will be locked.
     *
     * <p>Blocks until all locked tasks have completed execution,
     * or a timeout of 15 seconds occurs, or the current thread is
     * interrupted, whichever happens first.</p>
     */
    public void stop() {
        stop(15, TimeUnit.SECONDS);
    }

    /**
     * Sets the log level for the FailsafeExecutor.
     * This affects all components (FailsafeExecutor, WorkerPool, HeartbeatService).
     *
     * @param level The log level to set
     */
    public void setLogLevel(Level level) {
        Log.setLevel(level);
    }

    /**
     * Enables debug logging for the FailsafeExecutor.
     * This is a convenience method for troubleshooting issues.
     */
    public void enableDebugLogging() {
        Log.setLevel(Level.FINE);
    }

    /**
     * Enables error-only logging for the FailsafeExecutor.
     * This reduces log output to only show errors.
     */
    public void enableErrorOnlyLogging() {
        Log.setLevel(Level.SEVERE);
    }

    /**
     * Initiates an orderly shutdown in which previously locked tasks are executed,
     * but no new tasks will be locked.
     *
     * <p>Blocks until all locked tasks have completed execution,
     * or the provided timeout occurs, or the current thread is
     * interrupted, whichever happens first.</p>
     *
     * @param timeout  maximum time to block until all locked tasks have completed execution
     * @param timeUnit the unit of the given timeout
     */
    public void stop(long timeout, TimeUnit timeUnit) {
        try {
            logger.info("Stopping FailsafeExecutor with timeout: " + timeout + " " + timeUnit);

            if (executor == null) {
                return;
            }

            executor.shutdown();
            try {
                boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
                if (!terminated) {
                    logger.warn("Executor service did not terminate within the timeout period");
                }
            } catch (InterruptedException e) {
                logger.error("Executor service shutdown was interrupted", e);
                Thread.currentThread().interrupt(); // Preserve interrupt status
            }

            workerPool.stop(timeout, timeUnit);

            logger.info("FailsafeExecutor stopped successfully");
        } catch (Exception e) {
            logger.error("Error during FailsafeExecutor shutdown", e);
            throw e;
        }
    }

    /**
     * Restarts the executor after it has been stopped.
     * This creates a new ScheduledExecutorService and starts the worker pool again.
     */
    public void restart() {
        restart(null);
    }

    /**
     * Restarts the executor after it has been stopped.
     * This creates a new ScheduledExecutorService and starts the worker pool again.
     * If the executor is already running, it will be stopped first.
     *
     * @param nodeId id of the node that locks the given task
     */
    public void restart(String nodeId) {
        try {
            logger.info("Restarting FailsafeExecutor" + (nodeId != null ? " with nodeId: " + nodeId : ""));

            if (!executor.isShutdown()) {
                stop();
            } else {
                logger.debug("FailsafeExecutor is not running, proceeding with restart");
            }

            // Create a new executor since the old one cannot be restarted
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));

            // Start the executor with the provided nodeId
            start(nodeId);
        } catch (Exception e) {
            logger.error("Failed to restart FailsafeExecutor", e);
            throw e;
        }
    }

    /**
     * Registers the given function under the provided name.
     *
     * <p>Make sure your function is idempotent, since it gets executed at least once per task execution.</p>
     *
     * @param name     unique name of the task
     * @param function the function that should be assigned to the unique name, accepting a parameter.
     * @throws IllegalArgumentException if a task with the given name is already registered
     */
    public void registerTask(String name, TaskFunction<String> function) {
        if (taskRegistrationsByName.putIfAbsent(name, new TaskRegistration(name, function)) != null) {
            throw new IllegalArgumentException(String.format("Task '%s' is already registered", name));
        }

        taskNamesWithFunctions.add(name);
    }

    /**
     * Registers the given function under the provided name.
     *
     * <p>Before task execution a transaction is created. This transaction is committed after the given function executes without execptions. Furthermore the transaction is used to remove the task execution entry from database.</p>
     *
     * @param name     unique name of the task
     * @param function the function that should be assigned to the unique name, accepting a parameter.
     * @throws IllegalArgumentException if a task with the given name is already registered
     */
    public void registerTask(String name, TransactionalTaskFunction<String> function) {
        if (taskRegistrationsByName.putIfAbsent(name, new TaskRegistration(name, function)) != null) {
            throw new IllegalArgumentException(String.format("Task '%s' is already registered", name));
        }

        taskNamesWithFunctions.add(name);
    }

    /**
     * Registers a task under the provided name that runs remotely (in another FailsafeExecutor).
     *
     * @param name unique name of the task
     * @throws IllegalArgumentException if a task with the given name is already registered
     */
    public void registerRemoteTask(String name) {
        if (taskRegistrationsByName.putIfAbsent(name, new TaskRegistration(name)) != null) {
            throw new IllegalArgumentException(String.format("Task '%s' is already registered", name));
        }
    }

    /**
     * Persists a task in the database and executes the function assigned to the taskName at some time in the future.
     *
     * <p>A random taskId is assigned to this task. Thus a new task is definitely persisted in the database.</p>
     *
     * @param taskName  the name of the task that should be executed
     * @param parameter the parameter that should be passed to the function
     * @return taskId
     */
    public String execute(String taskName, String parameter) {
        return execute(UUID.randomUUID().toString(), taskName, parameter);
    }

    /**
     * Persists a task in the database and executes the function assigned to the taskName at some time in the future.
     *
     * <p>A random taskId is assigned to this task. Thus a new task is definitely persisted in the database.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection the JDBC connection used to persist the task in the database
     * @param taskName   the name of the task that should be executed
     * @param parameter  the parameter that should be passed to the function
     * @return taskId
     */
    public String execute(Connection connection, String taskName, String parameter) {
        return execute(connection, UUID.randomUUID().toString(), taskName, parameter);
    }

    /**
     * Persists a task in the database and executes the function assigned to the taskName at some time in the future.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * @param taskId    the id of the task used as unique constraint in database
     * @param taskName  the name of the task that should be executed
     * @param parameter the parameter that should be passed to the function
     * @return taskId or null if a task with the given taskId already exists
     */
    public String execute(String taskId, String taskName, String parameter) {
        return database.connect(connection -> execute(connection, taskId, taskName, parameter));
    }

    /**
     * Persists a task in the database and executes the function assigned to the taskName at some time in the future.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection the JDBC connection used to persist the task in the database
     * @param taskId     the id of the task used as unique constraint in database
     * @param taskName   the name of the task that should be executed
     * @param parameter  the parameter that should be passed to the function
     * @return taskId or null if a task with the given taskId already exists
     */
    public String execute(Connection connection, String taskId, String taskName, String parameter) {
        TaskRegistration taskRegistration = taskRegistrationsByName.get(taskName);
        if (taskRegistration == null) {
            throwTaskNotRegisteredException(taskName);
        }

        LocalDateTime plannedExecutionTime = systemClock.now();
        LocalDateTime now = plannedExecutionTime;
        if (taskRegistration.schedule != null) {
            plannedExecutionTime = taskRegistration.schedule.nextExecutionTime(plannedExecutionTime)
                    .orElseThrow(() -> new IllegalArgumentException(String.format("Schedule of task '%s' did not return an execution time for input '%s", taskName, now)));
        }

        Task taskInstance = new Task(taskId, taskName, parameter, plannedExecutionTime);
        return enqueue(connection, taskInstance);
    }

    /**
     * Persists a task in the database and defers execution of the function assigned to the taskName to the provided planned execution time.
     *
     * <p>A random taskId is assigned to this task. Thus a new task is definitely persisted in the database.</p>
     *
     * @param taskName             the name of the task that should be executed
     * @param parameter            the parameter that should be passed to the function
     * @param plannedExecutionTime the time when the task should be executed
     * @return taskId
     */
    public String defer(String taskName, String parameter, LocalDateTime plannedExecutionTime) {
        return defer(UUID.randomUUID().toString(), taskName, parameter, plannedExecutionTime);
    }

    /**
     * Persists a task in the database and defers execution of the function assigned to the taskName to the provided planned execution time.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * @param taskId               the id of the task used as unique constraint in database
     * @param taskName             the name of the task that should be executed
     * @param parameter            the parameter that should be passed to the function
     * @param plannedExecutionTime the time when the task should be executed
     * @return taskId or null if a task with the given taskId already exists
     */
    public String defer(String taskId, String taskName, String parameter, LocalDateTime plannedExecutionTime) {
        return database.connect(connection -> defer(connection, taskId, taskName, parameter, plannedExecutionTime));
    }

    /**
     * Persists a task in the database and defers execution of the function assigned to the taskName to the provided planned execution time.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection           the JDBC connection used to persist the task in the database
     * @param taskId               the id of the task used as unique constraint in database
     * @param taskName             the name of the task that should be executed
     * @param parameter            the parameter that should be passed to the function
     * @param plannedExecutionTime the time when the task should be executed
     * @return taskId or null if a task with the given taskId already exists
     */
    public String defer(Connection connection, String taskId, String taskName, String parameter, LocalDateTime plannedExecutionTime) {
        Task taskInstance = new Task(taskId, taskName, parameter, plannedExecutionTime);
        return enqueue(connection, taskInstance);
    }

    /**
     * Schedules the execution of the provided runnable. The task is then executed at the planned execution times
     * defined by the schedule.
     *
     * <p>With a {@link Schedule} you get a recurring execution as long as Schedule returns a {@link LocalDateTime}.</p>
     *
     * <p>If the runnable throws an exception the task is marked as failed and scheduling stops. Once the task successfully finishes by retrying it with {@link FailsafeExecutor#retry(Task)}, it gets scheduled for next execution.</p>
     *
     * <p>Make sure your runnable is idempotent, since it gets executed at least once per scheduled execution time.</p>
     *
     * @param taskName the name of the task that should be executed
     * @param schedule the schedule that defines the planned execution times
     * @param runnable the runnable
     * @return taskId
     */
    public String schedule(String taskName, Schedule schedule, Throwing.Runnable runnable) {
        return database.connect(connection -> schedule(connection, taskName, schedule, runnable));
    }

    /**
     * Schedules the execution of the provided runnable. The task is then executed at the planned execution times
     * defined by the schedule.
     *
     * <p>With a {@link Schedule} you get a recurring execution as long as Schedule returns a {@link LocalDateTime}.</p>
     *
     * <p>If the runnable throws an exception the task is marked as failed and scheduling stops. Once the task successfully finishes by retrying it with {@link FailsafeExecutor#retry(Task)}, it gets scheduled for next execution.</p>
     *
     * <p>Make sure your runnable is idempotent, since it gets executed at least once per scheduled execution time.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection the JDBC connection used to persist the task in the database
     * @param taskName   the name of the task that should be executed
     * @param schedule   the schedule that defines the planned execution times
     * @param runnable   the runnable to execute
     * @return taskId
     */
    public String schedule(Connection connection, String taskName, Schedule schedule, Throwing.Runnable runnable) {
        if (taskRegistrationsByName.putIfAbsent(taskName, new TaskRegistration(taskName, schedule, ignore -> runnable.run())) != null) {
            throw new IllegalArgumentException(String.format("Task '%s' is already registered", taskName));
        }

        taskNamesWithFunctions.add(taskName);

        LocalDateTime plannedExecutionTime = schedule.nextExecutionTime(systemClock.now())
                .orElseThrow(() -> new IllegalArgumentException("Schedule must return at least one execution time"));

        Task task = new Task(taskName, taskName, null, plannedExecutionTime);
        return enqueue(connection, task);
    }

    /**
     * Persists a task in the database and marks it as failed, so this task does not get executed. But it provides the possibility to retry or cancel the task.
     * This can be useful to make incidents during synchronous execution visible in the FailsafeExecutor context and reuse its retry mechanism.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param taskId    the id of the task used as unique constraint in database
     * @param taskName  the name of the task that should be executed
     * @param parameter the parameter that should be passed to the function
     * @param exception the exception to store
     * @return taskId
     */
    public String recordFailure(String taskId, String taskName, String parameter, Exception exception) {
        return database.connect(con -> recordFailure(con, taskId, taskName, parameter, exception));
    }

    /**
     * Persists a task in the database and marks it as failed, so this task does not get executed. But it provides the possibility to retry or cancel the task.
     * This can be useful to make incidents during synchronous execution visible in the FailsafeExecutor context and reuse its retry mechanism.
     *
     * <p>The taskId is used as unique constraint of this task. On conflict (task with this id already exists in database) insertion is simply skipped.
     * In this case no exception will be thrown. Method returns gracefully.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection the JDBC connection used to persist the task in the database
     * @param taskId     the id of the task used as unique constraint in database
     * @param taskName   the name of the task that should be executed
     * @param parameter  the parameter that should be passed to the function
     * @param exception  the exception to store
     * @return taskId
     */
    public String recordFailure(Connection connection, String taskId, String taskName, String parameter, Exception exception) {
        LocalDateTime now = systemClock.now();
        Task toSave = new Task(taskId, taskName, parameter, now, now, null, new ExecutionFailure(now, exception), 0, 0L);
        return taskRepository.add(connection, toSave);
    }

    /**
     * Returns the newest persisted tasks with a limit of 1000 rows.
     * <p>
     * The result set is ordered by CREATED_DATE DESC, ID DESC.
     *
     * @return list of all persisted tasks
     */
    public List<Task> findAll() {
        return taskRepository.findAll(null, null, null, 0, 1000);
    }

    /**
     * Returns all failed tasks with a limit of 1000 rows.
     * <p>
     * The result set is ordered by CREATED_DATE DESC, ID DESC.
     *
     * @return list of all failed tasks
     */
    public List<Task> findAllFailed() {
        return taskRepository.findAll(null, null, true, 0, 1000);
    }

    /**
     * Returns all tasks matching the criteria. A null value as parameter means not constraining the result.
     * <p>
     * If no sort order is specified then result set is ordered by CREATED_DATE DESC, ID DESC.
     *
     * @param taskName  finds the tasks by constraining the taskName
     * @param parameter finds the tasks by constraining the parameter
     * @param failed    finds the tasks by constraining if they are failed or not failed.
     * @param offset    offset to start from
     * @param limit     limit of the result set
     * @param sorts     sort order of the result set. See {@link Sort} for available sort criteria.
     * @return list of all persisted tasks
     */
    public List<Task> findAll(String taskName, String parameter, Boolean failed, int offset, int limit, Sort... sorts) {
        return taskRepository.findAll(taskName, parameter, failed, offset, limit, sorts);
    }

    /**
     * Returns all tasks matching the criteria. A null value as parameter means not constraining the result.
     * <p>
     * If no sort order is specified then result set is ordered by CREATED_DATE DESC, ID DESC.
     *
     * @param taskName          finds the tasks by constraining the taskName
     * @param parameter         finds the tasks by constraining the parameter
     * @param failed            finds the tasks by constraining if they are failed or not failed.
     * @param errorMessage      finds the tasks by constraining the error message contains text (case-insensitive)
     * @param createdDateFromInclusive   finds the tasks by constraining from created date (inclusive).
     * @param createdDateToExclusive     finds the tasks by constraining to created date (exclusive).
     * @param failureDateFromInclusive   finds the tasks by constraining from failure date (inclusive).
     * @param failureDateToExclusive     finds the tasks by constraining to failure date (exclusive).
     * @param offset            offset to start from
     * @param limit             limit of the result set
     * @param sorts             sort order of the result set. See {@link Sort} for available sort criteria.
     * @return list of all persisted tasks
     */
    public List<Task> findAll(String taskName,
                              String parameter,
                              Boolean failed,
                              String errorMessage,
                              LocalDateTime createdDateFromInclusive,
                              LocalDateTime createdDateToExclusive,
                              LocalDateTime failureDateFromInclusive,
                              LocalDateTime failureDateToExclusive,
                              int offset,
                              int limit,
                              Sort... sorts) {
        return taskRepository.findAll(
                taskName,
                parameter,
                failed,
                errorMessage,
                createdDateFromInclusive,
                createdDateToExclusive,
                failureDateFromInclusive,
                failureDateToExclusive,
                offset,
                limit,
                sorts);
    }

    /**
     * Returns a single task.
     *
     * @param taskId the id of the task
     * @return the task
     */
    public Optional<Task> findOne(String taskId) {
        return Optional.ofNullable(taskRepository.findOne(taskId));
    }

    /**
     * Returns the count of all tasks.
     *
     * @return count of all tasks
     */
    public int count() {
        return taskRepository.count(null, null, null);
    }

    /**
     * Returns the count of all failed tasks.
     *
     * @return count of all failed tasks
     */
    public int countFailedTasks() {
        return taskRepository.count(null, null, true);
    }

    /**
     * Returns the count of all tasks matching the criteria. A null value as parameter means not constraining the result.
     *
     * @param taskName  counts the tasks by constraining the taskName
     * @param parameter counts the tasks by constraining the parameter
     * @param failed    counts the tasks by constraining if they are failed or not failed.
     * @return count of all tasks
     */
    public int count(String taskName, String parameter, Boolean failed) {
        return taskRepository.count(taskName, parameter, failed);
    }

    public boolean retry(Task failedTask) {
        return database.connect(con -> retry(con, failedTask));
    }

    public boolean retry(Connection con, Task failedTask) {
        if (failedTask.isRetryable()) {
            listeners.forEach(listener -> listener.retrying(failedTask.getName(), failedTask.getId(), failedTask.getParameter()));
            taskRepository.deleteFailure(con, failedTask);
            return true;
        }

        return false;
    }

    public boolean cancel(Task failedTask) {
        return database.connect(con -> cancel(con, failedTask));
    }

    public boolean cancel(Connection con, Task failedTask) {
        if (failedTask.isCancelable()) {
            taskRepository.delete(con, failedTask);
            return true;
        }

        return false;
    }

    /**
     * Registers a listener to observe task execution.
     *
     * @param listener the listener to register
     */
    public void subscribe(TaskExecutionListener listener) {
        listeners.add(listener);
    }

    /**
     * Removes the given listener from the list of observers.
     *
     * @param listener the listener to remove
     */
    public void unsubscribe(TaskExecutionListener listener) {
        listeners.remove(listener);
    }

    /**
     * Returns if last run of {@link FailsafeExecutor} was successful.
     *
     * @return true if no exception occured during last run of {@link FailsafeExecutor}
     */
    public boolean isLastRunFailed() {
        return lastRunException != null;
    }

    /**
     * Returns the exception of last run of {@link FailsafeExecutor} if an error occured.
     *
     * @return the exception of last run or null if last run was successful.
     */
    public Exception lastRunException() {
        return lastRunException;
    }

    /**
     * Register an observer to make selection/query behavior of the persistent queue visible.
     *
     * @param observer the callback method that receives the latest queue selection results.
     */
    public void observeQueue(PersistentQueueObserver observer) {
        persistentQueue.setObserver(observer);
    }

    /**
     * Removes the queue observer. Callbacks are stopped.
     */
    public void stopQueueObservation() {
        persistentQueue.setObserver(null);
    }

    private String enqueue(Connection connection, Task task) {
        if (!taskRegistrationsByName.containsKey(task.getName())) {
            throwTaskNotRegisteredException(task.getName());
        }

        notifyPersisting(task, task.getId());
        return persistentQueue.add(connection, task);
    }

    private void executeNextTasks() {
        try {
            int freeSlots = workerPool.freeSlots();
            if (freeSlots <= 0) {
                logger.trace("No free queue slots available, skipping task execution");
                return;
            }

            List<Task> toExecute = persistentQueue.peekAndLock(taskNamesWithFunctions, freeSlots, nodeId);
            if (toExecute.isEmpty()) {
                logger.trace("No tasks to execute");
                return;
            }

            logger.trace("Executing " + toExecute.size() + " tasks");
            for (Task task : toExecute) {
                try {
                    TaskRegistration registration = taskRegistrationsByName.get(task.getName());
                    Execution execution = new Execution(database, task, registration, listeners, systemClock, taskRepository);
                    workerPool.execute(task, execution::perform);
                } catch (Exception e) {
                    logger.error("Failed to submit task: " + task.getName() + " (ID: " + task.getId() + ")", e);
                    // Continue with other tasks even if one fails
                }
            }

            clearException();

        } catch (Exception e) {
            storeException(e);
        }
    }

    void storeException(Exception e) {
        logger.error("An error occurred in FailsafeExecutor", e);
        lastRunException = e;
    }

    void clearException() {
        lastRunException = null;
    }

    private void notifyPersisting(Task task, String taskId) {
        String name = task.getName();
        String parameter = task.getParameter();
        listeners.forEach(listener -> listener.persisting(name, taskId, parameter));
    }

    private void validateDatabase(DataSource dataSource) throws SQLException {
        List<Task> tasks = new ArrayList<>();

        // validate table structure (with persist)
        try (Connection connection = dataSource.getConnection();
             Transaction transaction = new Transaction(connection)) { // no commit of trx
            tasks.add(new Task(UUID.randomUUID().toString(), "validateDatabaseTableTaskName", null, systemClock.now()));
            tasks.add(new Task(UUID.randomUUID().toString(), "validateDatabaseTableTaskName", null, systemClock.now()));
            for (Task task : tasks) {
                if (taskRepository.add(connection, task) == null) {
                    throw new IllegalStateException("Validation of database failed! Unable to persist tasks!");
                }
            }
            transaction.commit();
        }

        // validate execute batch return result (with lock)
        // check SUCCESS_NO_INFO is not returned by jdbc driver
        // see https://jira.mariadb.org/browse/CONJ-920
        try (Connection connection = dataSource.getConnection();
             Transaction transaction = new Transaction(connection)) { // no commit of trx
            tasks = taskRepository.lock(connection, tasks, null);
            if (tasks.size() != 2) {
                throw new IllegalStateException("Validation of database failed! Unable to lock tasks!");
            }
            transaction.commit();
        }

        // cleanup
        try (Connection connection = dataSource.getConnection();
             Transaction transaction = new Transaction(connection)) { // no commit of trx
            for (Task task : tasks) {
                taskRepository.delete(connection, task);
            }
            transaction.commit();
        }
    }

    private String throwTaskNotRegisteredException(String taskName) {
        throw new IllegalArgumentException(String.format("Task '%s' not registered. Use 'registerTask' if the task should run locally or 'registerRemoteTask' if the task should run remotely.", taskName));
    }

    static class TaskRegistration {

        final String name;
        final Schedule schedule;
        final TaskFunction<String> function;
        final TransactionalTaskFunction<String> transactionalFunction;

        TaskRegistration(String name) {
            this.name = name;
            this.schedule = null;
            this.function = null;
            this.transactionalFunction = null;
        }

        TaskRegistration(String name, TaskFunction<String> function) {
            this.name = name;
            this.schedule = null;
            this.function = function;
            this.transactionalFunction = null;
        }

        TaskRegistration(String name, TransactionalTaskFunction<String> transactionalFunction) {
            this.name = name;
            this.schedule = null;
            this.function = null;
            this.transactionalFunction = transactionalFunction;
        }

        TaskRegistration(String name, Schedule schedule, TaskFunction<String> function) {
            this.name = name;
            this.schedule = schedule;
            this.function = function;
            this.transactionalFunction = null;
        }

        boolean requiresTransaction() {
            return transactionalFunction != null;
        }

        boolean isScheduled() {
            return schedule != null;
        }

        boolean isRegularTask() {
            return !isScheduled() && !requiresTransaction();
        }
    }
}
