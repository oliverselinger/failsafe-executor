package os.failsafe.executor;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FailsafeExecutor {

    private static final Logger log = Logger.getLogger(FailsafeExecutor.class.getName());
    public static final String QUEUE_PREFIX = "FE_QUEUE_";

    private final SystemClock systemClock;
    private final Duration initialDelay;
    final Duration pollingInterval;
    private final Duration lockTimeout;
    final Duration heartbeatInterval;
    final Duration queuePollingInterval;
    private final String nodeId;
    private final int maxWorkers;
    private final int queueSize;
    private final DataSource dataSource;

    private ExecutorService executor;
    private ScheduledExecutorService scheduler;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile boolean running = false;

    private final Map<String, TaskRegistration> taskRegistry = new ConcurrentHashMap<>();
    private final Map<String, Schedule> scheduleRegistry = new ConcurrentHashMap<>();
    final Set<String> activeTaskIds = ConcurrentHashMap.newKeySet();
    private final List<TaskExecutionListener> listeners = new CopyOnWriteArrayList<>();

    private volatile Consumer<Throwable> uncaughtErrorHandler = (e) -> {
    }; // default no-op

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout, Duration queuePollingInterval, String nodeId) throws SQLException {
        this.systemClock = systemClock;
        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
        this.nodeId = nodeId;
        this.maxWorkers = workerThreadCount;
        this.lockTimeout = lockTimeout;
        this.heartbeatInterval = Duration.ofMillis(lockTimeout.toMillis() / 3);
        this.queuePollingInterval = queuePollingInterval;
        this.queueSize = queueSize;
        this.dataSource = dataSource;
        validateDatabase();
    }

    /**
     * Start execution of any submitted tasks.
     */
    public synchronized void start() {
        if (running) {
            FailsafeExecutor.log.warning("FailsafeExecutor already running.");
            return;
        }
        running = true;

        this.executor = Executors.newFixedThreadPool(maxWorkers);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleWithFixedDelay(this::pollAndSubmit, initialDelay.toMillis(), pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
        heartbeatExecutor.scheduleWithFixedDelay(this::extendLeasesBatch, initialDelay.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("FailsafeExecutor started.");
    }

    /**
     * Initiates an orderly shutdown in which previously locked tasks are executed, but no new tasks will be locked.
     * <p>Blocks until all locked tasks have completed execution, or a timeout occurs, or the current thread is interrupted, whichever happens first.</p>
     */
    public synchronized void shutdown() {
        if (!running) return;
        running = false;
        log.info("Shutting down FailsafeExecutor...");
        scheduler.shutdown();
        heartbeatExecutor.shutdown();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow();
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) scheduler.shutdownNow();
            if (!heartbeatExecutor.awaitTermination(10, TimeUnit.SECONDS)) heartbeatExecutor.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("FailsafeExecutor stopped.");
    }

    /**
     * Indicates whether the executor and all its executor services are running.
     */
    public boolean isRunning() {
        return running && !executor.isShutdown() && !scheduler.isShutdown() && !heartbeatExecutor.isShutdown();
    }

    /**
     * Registers a custom error handler that will be invoked for uncaught errors encountered during task execution.
     */
    public void registerErrorHandler(Consumer<Throwable> handler) {
        this.uncaughtErrorHandler = handler;
    }

    /**
     * Registers the given function under the provided name. The function defines the logic to be executed for the task.
     */
    public void registerTask(String name, TaskFunction function) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(function, null)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    /**
     * Registers a queue under the provided name. Every task assigned to this queue is executed sequentially.
     */
    public void registerQueue(String queueName) throws SQLException {
        schedule(QUEUE_PREFIX + queueName, queueName, new DurationSchedule(queuePollingInterval), this::pollQueue);
    }

    /**
     * Registers the given function under the provided name. The function defines the logic to be executed for the task.
     * <p>Before task execution a transaction is created. This transaction is committed after the given function executes without exceptions. Furthermore, the transaction is used to remove the failsafe_task from the database.</p>
     */
    public void registerTask(String name, TransactionalTaskFunction function) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(null, function)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    /**
     * Registers a task under the provided name that runs remotely on a different node (in another FailsafeExecutor).
     */
    public void registerRemoteTask(String name) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(null, null)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    /**
     * Registers a listener to observe task execution.
     */
    public void subscribe(TaskExecutionListener listener) {
        listeners.add(listener);
    }

    /**
     * Removes the given listener from the list of observers.
     */
    public void unsubscribe(TaskExecutionListener listener) {
        listeners.remove(listener);
    }

    /**
     * Schedules a task for execution based on the provided schedule.
     */
    public void schedule(String name, Schedule schedule, ThrowingRunnable runnable) throws SQLException {
        schedule(name, null, schedule, ignore -> runnable.run());
    }

    /**
     * Schedules a task for execution based on the provided schedule.
     */
    public void schedule(String name, String parameter, Schedule schedule, TaskFunction fn) throws SQLException {
        registerTask(name, fn);
        if (scheduleRegistry.putIfAbsent(name, schedule) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }

        LocalDateTime plannedExecutionTime = schedule.nextExecutionTime(systemClock.now())
                .orElseThrow(() -> new IllegalArgumentException("Schedule must return at least one execution time"));

        try (Connection connection = dataSource.getConnection()) {
            execute(connection, null, name, name, parameter, plannedExecutionTime);
        }
    }

    /**
     * Defers the execution of a task to a specified planned execution time.
     */
    public String defer(String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            execute(conn, null, id, taskName, parameter, plannedExecutionTime);
        }
        return id;
    }

    /**
     * Defers the execution of a task to a specified planned execution time.
     */
    public void defer(String id, String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            execute(conn, null, id, taskName, parameter, plannedExecutionTime);
        }
    }

    /**
     * Defers the execution of a task to a specified planned execution time.
     */
    public void defer(Connection conn, String id, String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        execute(conn, null, id, taskName, parameter, plannedExecutionTime);
    }

    /**
     * Submits a task for execution with the provided task name and parameter.
     * This method generates a unique identifier for the task and assigns the current system time as the planned execution time.
     */
    public String execute(String taskName, String parameter) throws SQLException {
        String id = UUID.randomUUID().toString();
        try (Connection conn = dataSource.getConnection()) {
            execute(conn, null, id, taskName, parameter, systemClock.now());
        }
        return id;
    }

    /**
     * Submits a task for execution with the provided task name and parameter.
     * This method generates a unique identifier for the task and assigns the current system time as the planned execution time.
     */
    public String execute(Connection conn, String taskName, String parameter) throws SQLException {
        String id = UUID.randomUUID().toString();
        execute(conn, null, id, taskName, parameter, systemClock.now());
        return id;
    }

    /**
     * Submits a task for execution with the specified task identifier, name, and parameter values.
     * The planned execution time is set to the current system time during the invocation. If a task with the given identifier already exists, it will not
     * create a new one in the database.
     */
    public void execute(String id, String taskName, String parameter) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            execute(conn, null, id, taskName, parameter, systemClock.now());
        }
    }

    /**
     * Submits a task for execution with the specified task identifier, name, and parameter values.
     * The planned execution time is set to the current system time during the invocation. If a task with the given identifier already exists, it will not
     * create a new one in the database.
     */
    public void execute(Connection conn, String id, String taskName, String parameter) throws SQLException {
        execute(conn, null, id, taskName, parameter, systemClock.now());
    }

    /**
     * Submits a task for execution to the provided queue with the specified task identifier, name, and parameter values.
     * The planned execution time is set to the current system time during the invocation. If a task with the given identifier already exists, it will not
     * create a new one in the database.
     */
    public void execute(Connection conn, String queueName, String id, String taskName, String parameter) throws SQLException {
        execute(conn, queueName, id, taskName, parameter, systemClock.now());
    }

    /**
     * Submits a task for execution with the specified task identifier, name, parameter values,
     * and planned execution time. If a task with the given identifier already exists, it will not
     * create a new one in the database.
     */
    public void execute(Connection conn, String id, String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        execute(conn, null, id, taskName, parameter, plannedExecutionTime);
    }

    /**
     * Submits a task for execution with the specified task identifier, name, parameter values,
     * and planned execution time. If a task with the given identifier already exists, it will not
     * create a new one in the database.
     */
    public void execute(Connection conn, String queueName, String id, String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        if(queueName != null && !scheduleRegistry.containsKey(QUEUE_PREFIX + queueName)) {
            throw new IllegalArgumentException("Queue '%s' is not registered".formatted(queueName));
        }
        notifyPersisting(taskName, id, parameter, plannedExecutionTime);
        String sql = """
                INSERT INTO FAILSAFE_TASK (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE, QUEUE_NAME)
                    SELECT ?, ?, ?, ?, CURRENT_TIMESTAMP, ? FROM DUAL
                    WHERE NOT EXISTS (SELECT 1 FROM FAILSAFE_TASK WHERE ID = ?)
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, taskName);
            stmt.setString(3, parameter);
            stmt.setTimestamp(4, Timestamp.valueOf(plannedExecutionTime.truncatedTo(ChronoUnit.MICROS)));
            stmt.setString(5, queueName);
            stmt.setString(6, id);
            stmt.executeUpdate();
        }
        log.info("Task submitted: " + taskName + " (" + id + ") - parameter: " +  parameter + (queueName != null ? " - queue: " + queueName : ""));
    }

    public boolean retry(Task task) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return retry(conn, task);
        }
    }

    public boolean retry(Connection tx, Task task) throws SQLException {
        String sql = """
                UPDATE FAILSAFE_TASK
                   SET FAIL_TIME = NULL,
                       LOCK_TIME = NULL,
                       NODE_ID = NULL,
                       RETRY_COUNT = RETRY_COUNT + 1,
                       EXCEPTION_MESSAGE = NULL,
                       STACK_TRACE = NULL
                 WHERE ID = ? AND FAIL_TIME IS NOT NULL
                """;

        notifyRetrying(new Task(task.id, task.name, task.parameter));

        try (PreparedStatement stmt = tx.prepareStatement(sql)) {
            stmt.setString(1, task.id);
            int updated = stmt.executeUpdate();

            return updated == 1;
        }
    }

    /**
     * Records a task failure in the database table if it is not already present.
     *  <p>This task does not get executed. But it provides the possibility to retry or cancel the task.</p>
     */
    public boolean recordFailure(String taskId, String taskName, String parameter, Exception exception) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            return recordFailure(connection, taskId, taskName, parameter, exception);
        }
    }

    /**
     * Records a task failure in the database table if it is not already present.
     * <p>This task does not get executed. But it provides the possibility to retry or cancel the task.</p>
     */
    public boolean recordFailure(Connection tx, String taskId, String taskName, String parameter, Exception exception) throws SQLException {
        String sql = """
                INSERT INTO FAILSAFE_TASK (ID, NAME, PARAMETER, CREATED_DATE, PLANNED_EXECUTION_TIME, FAIL_TIME, EXCEPTION_MESSAGE, STACK_TRACE)
                    SELECT ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ? FROM DUAL
                    WHERE NOT EXISTS (SELECT 1 FROM FAILSAFE_TASK WHERE ID = ?)
                """;

        try (PreparedStatement stmt = tx.prepareStatement(sql)) {
            LocalDateTime now = systemClock.now().truncatedTo(ChronoUnit.MICROS);
            stmt.setString(1, taskId);
            stmt.setString(2, taskName);
            stmt.setString(3, parameter);
            stmt.setTimestamp(4, Timestamp.valueOf(now));
            stmt.setTimestamp(5, Timestamp.valueOf(now));
            stmt.setString(6, exception != null ? exception.getMessage() : null);
            stmt.setString(7, exception != null ? stackTraceToString(exception) : null);
            stmt.setString(8, taskId);
            int updated = stmt.executeUpdate();
            return updated == 1;
        }
    }

    public void cancel(Task task) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            delete(conn, task.id);
        }
    }

    public Optional<Task> findOne(String id) throws SQLException {
        String sql = "SELECT * FROM FAILSAFE_TASK WHERE ID=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Optional.of(Task.from(rs));
            }
        }
        return Optional.empty();
    }

    public List<Task> findAll(Boolean failed) throws SQLException {
        List<Task> tasks = new ArrayList<>();
        String sql = "SELECT * FROM FAILSAFE_TASK";
        if (failed != null && failed) sql += " WHERE FAIL_TIME IS NOT NULL";
        if (failed != null && !failed) sql += " WHERE FAIL_TIME IS NULL";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                tasks.add(Task.from(rs));
            }
        }
        return tasks;
    }

    // -------------------- INTERNAL EXECUTION --------------------

    private void pollAndSubmit() {
        if (!running) return;
        try {
            int freeSlots = Math.max(0, queueSize - activeTaskIds.size());
            if (freeSlots > 0) {
                pickTasks(queueSize).forEach(task -> {
                    activeTaskIds.add(task.id);
                    executor.submit(() -> {
                        try {
                            executeTask(task);
                        } catch (Exception e) {
                            uncaughtErrorHandler.accept(e);
                        }
                    });
                });
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Polling error", e);
            uncaughtErrorHandler.accept(e);
        }
    }

    private List<Task> pickTasks(int limit) throws SQLException {
        if (taskRegistry.isEmpty()) return Collections.emptyList();
        Set<String> registeredTaskNames = taskRegistry.keySet().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
        String processableTasksPlaceHolder = IntStream.range(0, registeredTaskNames.size()).mapToObj(s -> "?").collect(Collectors.joining(","));

        List<Task> tasks = new ArrayList<>();
        String sql = """
                SELECT ID, NAME, PARAMETER, LOCK_TIME, NODE_ID
                  FROM FAILSAFE_TASK
                 WHERE (LOCK_TIME IS NULL OR LOCK_TIME < ?)
                   AND FAIL_TIME IS NULL
                   AND QUEUE_NAME IS NULL
                   AND PLANNED_EXECUTION_TIME <= ?
                   AND NAME IN (%s) 
                   AND ROWNUM <= ? 
                 ORDER BY PLANNED_EXECUTION_TIME
                 FOR UPDATE SKIP LOCKED
                """;

        sql = sql.formatted(processableTasksPlaceHolder);

        String updateSql = "UPDATE FAILSAFE_TASK SET LOCK_TIME = ?, NODE_ID = ? WHERE ID = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql);
             PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {
            connection.setAutoCommit(false);

            LocalDateTime now = systemClock.now().truncatedTo(ChronoUnit.MICROS);
            stmt.setTimestamp(1, Timestamp.valueOf(now.minus(lockTimeout)));
            stmt.setTimestamp(2, Timestamp.valueOf(now));
            int cnt = 3;
            for (String taskName : registeredTaskNames) {
                stmt.setString(cnt++, taskName);
            }
            stmt.setInt(cnt, limit);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString("ID");
                String name = rs.getString("NAME");
                String parameter = rs.getString("PARAMETER");
                updateStmt.setTimestamp(1, Timestamp.valueOf(now));
                updateStmt.setString(2, nodeId);
                updateStmt.setString(3, id);
                updateStmt.addBatch();
                tasks.add(new Task(id, name, parameter));
            }

            if (!tasks.isEmpty()) updateStmt.executeBatch();

            connection.commit();
        }

        if (!tasks.isEmpty()) log.fine("Picked " + tasks.size() + " task(s).");
        return tasks;
    }

    private void executeTask(Task task) throws SQLException {
        LocalDateTime now = systemClock.now().truncatedTo(ChronoUnit.MICROS);
        log.info("Executing task: " + task.name + " (" + task.id + ")");
        try {
            String sql = "UPDATE FAILSAFE_TASK SET START_TIME = ? WHERE ID = ?";
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setTimestamp(1, Timestamp.valueOf(now));
                stmt.setString(2, task.id);
                stmt.executeUpdate();
            }

            TaskRegistration taskRegistration = taskRegistry.get(task.name);

            if (taskRegistration.function != null) {
                taskRegistration.function.accept(task.parameter);
            }

            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);

                if (taskRegistration.trxConsumingFunction != null) {
                    taskRegistration.trxConsumingFunction.accept(connection, task.parameter);
                }

                Schedule schedule = scheduleRegistry.get(task.name);
                if (schedule != null) {
                    Optional<LocalDateTime> next = schedule.nextExecutionTime(systemClock.now());
                    if (next.isPresent()) rescheduleTask(connection, task.id, Timestamp.valueOf(next.get()));
                    else delete(connection, task.id);
                } else delete(connection, task.id);

                connection.commit();
            }

            notifySucceeded(task);

        } catch (Exception e) {
            markFailed(task.id, e);
            notifyFailed(task, e);
        } finally {
            activeTaskIds.remove(task.id);
        }
    }

    private void extendLeasesBatch() {
        if (activeTaskIds.isEmpty() || !running) return;
        String sql = "UPDATE FAILSAFE_TASK SET LOCK_TIME = ? WHERE NODE_ID = ? AND ID = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);

            LocalDateTime now = systemClock.now().truncatedTo(ChronoUnit.MICROS);
            for (String id : activeTaskIds) {
                stmt.setTimestamp(1, Timestamp.valueOf(now));
                stmt.setString(2, nodeId);
                stmt.setString(3, id);
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            log.log(Level.WARNING, "Heartbeat extension failed", e);
            uncaughtErrorHandler.accept(e);
        }
    }


    private void pollQueue(String queueName) {
        if (!running) return;

        try {
            List<Task> tasks;
            while (!(tasks = fetchQueuedTasks(queueName)).isEmpty()) {
                for (Task task : tasks) {
                    if(!running) { // break out if shutdown got called
                        return;
                    }

                    try {
                        executeTask(task);
                    } catch (Exception e) {
                        log.log(Level.SEVERE, "Queue execution error", e);
                        uncaughtErrorHandler.accept(e);
                    }
                }
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Queue polling error", e);
            uncaughtErrorHandler.accept(e);
        }
    }

    private List<Task> fetchQueuedTasks(String queueName) throws SQLException {
        Set<String> registeredTaskNames = taskRegistry.keySet().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
        String processableTasksPlaceHolder = IntStream.range(0, registeredTaskNames.size()).mapToObj(s -> "?").collect(Collectors.joining(","));

        List<Task> tasks = new ArrayList<>();

        String sql = """
                SELECT ID, NAME, PARAMETER 
                    FROM FAILSAFE_TASK
                WHERE QUEUE_NAME = ?
                    AND FAIL_TIME IS NULL
                    AND PLANNED_EXECUTION_TIME <= ?
                    AND NAME IN (%s) 
                ORDER BY PLANNED_EXECUTION_TIME
                """;

        sql = sql.formatted(processableTasksPlaceHolder);

        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(sql)) {

            LocalDateTime now = systemClock.now().truncatedTo(ChronoUnit.MICROS);

            stmt.setString(1, queueName);
            stmt.setTimestamp(2, Timestamp.valueOf(now));
            int cnt = 3;
            for (String taskName : registeredTaskNames) {
                stmt.setString(cnt++, taskName);
            }

            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String id = rs.getString("ID");
                String name = rs.getString("NAME");
                String parameter = rs.getString("PARAMETER");
                tasks.add(new Task(id, name, parameter));
            }
        }
        return tasks;
    }

    // -------------------- TASK STATE --------------------

    private void rescheduleTask(Connection connection, String taskId, Timestamp nextRun) throws SQLException {
        String sql = "UPDATE FAILSAFE_TASK SET PLANNED_EXECUTION_TIME=?, LOCK_TIME=NULL, NODE_ID=NULL WHERE ID=?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setTimestamp(1, nextRun);
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    private void delete(Connection connection, String taskId) throws SQLException {
        String sql = "DELETE FROM FAILSAFE_TASK WHERE ID=?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, taskId);
            stmt.executeUpdate();
        }
    }

    private void markFailed(String taskId, Exception ex) throws SQLException {
        String sql = """
                UPDATE FAILSAFE_TASK
                   SET FAIL_TIME = ?,
                       EXCEPTION_MESSAGE = ?,
                       STACK_TRACE = ?
                 WHERE ID = ?
                """;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(true);
            stmt.setTimestamp(1, Timestamp.valueOf(systemClock.now().truncatedTo(ChronoUnit.MICROS)));
            stmt.setString(2, ex.getMessage());
            stmt.setString(3, stackTraceToString(ex));
            stmt.setString(4, taskId);
            stmt.executeUpdate();
        }
    }

    // -------------------- HELPERS --------------------

    private String stackTraceToString(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private void validateDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            execute(connection, UUID.randomUUID().toString(), "validateDatabaseTable", null);
            connection.rollback();
        }
    }

    // -------------------- OBSERVER CALLS --------------------

    private void notifyPersisting(String name, String id, String param, LocalDateTime plannedExecutionTime) {
        listeners.forEach(l -> safeInvoke(() -> l.persisting(name, id, param, plannedExecutionTime)));
    }

    private void notifyRetrying(Task task) {
        listeners.forEach(l -> safeInvoke(() -> l.retrying(task.name, task.id, task.parameter)));
    }

    private void notifySucceeded(Task task) {
        listeners.forEach(l -> safeInvoke(() -> l.succeeded(task.name, task.id, task.parameter)));
    }

    private void notifyFailed(Task task, Exception ex) {
        listeners.forEach(l -> safeInvoke(() -> l.failed(task.name, task.id, task.parameter, ex)));
    }

    private void safeInvoke(Runnable r) {
        try {
            r.run();
        } catch (Exception e) {
            log.log(Level.WARNING, "Listener threw exception", e);
            uncaughtErrorHandler.accept(e);
        }
    }

    // -------------------- STRUCTS --------------------

    private record TaskRegistration(TaskFunction function, TransactionalTaskFunction trxConsumingFunction) {
    }

    public interface SystemClock {
        LocalDateTime now();
    }

    public interface Schedule {
        /**
         * Defines a schedule for executing a task, which can be either a one-time or a recurring execution.
         * <p><b>One-time execution:</b> Return {@link Optional#empty()} after the task has been executed, indicating that no further executions are planned.</p>
         * <p><b>Recurring execution:</b> Always return an {@link Optional} containing the next scheduled execution time. For an example, see {@link DailySchedule}.</p>
         */
        Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime);
    }

    public interface TaskExecutionListener {
        void persisting(String name, String id, String parameter, LocalDateTime plannedExecutionTime);
        void retrying(String name, String id, String parameter);
        void succeeded(String name, String id, String parameter);
        void failed(String name, String id, String parameter, Exception exception);
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    public interface TaskFunction {
        void accept(String param) throws Exception;
    }

    public interface TransactionalTaskFunction {
        void accept(Connection connection, String param) throws Exception;
    }

    public record Task(String id, String name, String parameter, String nodeId, String queueName, LocalDateTime creationTime,
                       LocalDateTime plannedExecutionTime, LocalDateTime lockTime, LocalDateTime startTime, ExecutionFailure executionFailure,
                       int retryCount) {

        public Task(String id, String name, String parameter) {
            this(id, name, parameter, null, null, null, null, null, null, null, 0);
        }

        public boolean isLocked() {
            return lockTime != null;
        }

        public boolean isExecutionFailed() {
            return executionFailure != null;
        }

        public boolean isCancelable() {
            return !isLocked();
        }

        public boolean isRetryable() {
            return isExecutionFailed();
        }

        public static Task from(ResultSet rs) throws SQLException {
            Timestamp lockTime = rs.getTimestamp("LOCK_TIME");
            Timestamp plannedExecutionTime = rs.getTimestamp("PLANNED_EXECUTION_TIME");
            Timestamp startTime = rs.getTimestamp("START_TIME");
            return new Task(
                    rs.getString("ID"),
                    rs.getString("NAME"),
                    rs.getString("PARAMETER"),
                    rs.getString("NODE_ID"),
                    rs.getString("QUEUE_NAME"),
                    rs.getTimestamp("CREATED_DATE").toLocalDateTime(),
                    plannedExecutionTime != null ? plannedExecutionTime.toLocalDateTime() : null,
                    lockTime != null ? lockTime.toLocalDateTime() : null,
                    startTime != null ? startTime.toLocalDateTime() : null,
                    ExecutionFailure.from(rs),
                    rs.getInt("RETRY_COUNT"));
        }
    }

    public record ExecutionFailure(LocalDateTime failTime, String exceptionMessage, String stackTrace) {

        public static ExecutionFailure from(ResultSet rs) throws SQLException {
            Timestamp failTime = rs.getTimestamp("FAIL_TIME");
            if (rs.wasNull()) {
                return null;
            }
            String exceptionMessage = rs.getString("EXCEPTION_MESSAGE");
            StringWriter writer = new StringWriter();
            try {
                rs.getCharacterStream("STACK_TRACE").transferTo(writer);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            String stackTrace = writer.toString();

            return new ExecutionFailure(failTime.toLocalDateTime(), exceptionMessage, stackTrace);
        }
    }

    public static <T> TaskWaitResult<T> waitForTasks(FailsafeExecutor failsafeExecutor,  Duration timeout, ThrowingRunnable taskProducer) throws Exception {
        return waitForTasks(failsafeExecutor, timeout, () -> { taskProducer.run(); return null;});
    }

    public static <T> TaskWaitResult<T> waitForTasks(FailsafeExecutor failsafeExecutor, Duration timeout, ThrowingSupplier<T> taskProducer) throws Exception {
        Map<String, LocalDateTime> activeTasks = new ConcurrentHashMap<>();
        List<Task> failures = Collections.synchronizedList(new ArrayList<>());
        Object lock = new Object();
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        TaskExecutionListener listener = new TaskExecutionListener() {
            public void persisting(String n, String i, String p, LocalDateTime plannedExecutionTime) {
                synchronized (lock) {
                    activeTasks.put(i+n, plannedExecutionTime.truncatedTo(ChronoUnit.MICROS));
                }
            }
            public void retrying(String n, String i, String p) {
                synchronized (lock) {
                    activeTasks.put(i+n, failsafeExecutor.systemClock.now().truncatedTo(ChronoUnit.MICROS));
                }
            }
            public void succeeded(String n, String i, String p) { done(i+n); }
            public void failed(String n, String i, String p, Exception e) {
                failures.add(new Task(i, n, p));
                done(i+n);
            }
            private void done(String id) {
                synchronized (lock) {
                    if(activeTasks.remove(id) != null) {
                        var now = failsafeExecutor.systemClock.now();
                        if (!hasDueTasks(activeTasks, now))
                            lock.notifyAll();
                    }
                }
            }
        };

        failsafeExecutor.findAll(false).stream().filter(task -> failsafeExecutor.taskRegistry.containsKey(task.name)).forEach(task -> listener.persisting(task.name, task.id, task.parameter, task.plannedExecutionTime));

        failsafeExecutor.subscribe(listener);
        try {
            T value = taskProducer.get();
            synchronized (lock) {
                while (hasDueTasks(activeTasks, failsafeExecutor.systemClock.now().truncatedTo(ChronoUnit.MICROS))) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        throw new TimeoutException("Timed out waiting for tasks to complete.");
                    }
                    lock.wait(remaining);
                }
            }

            return new TaskWaitResult<>(value, failures);
        } finally {
            failsafeExecutor.unsubscribe(listener);
        }
    }

    private static boolean hasDueTasks(Map<String, LocalDateTime> activeTasks, LocalDateTime now) {
        return activeTasks.entrySet().stream()
                .anyMatch(entry -> entry.getValue().isBefore(now) || entry.getValue().isEqual(now));
    }

    public record TaskWaitResult<T> (T value, List<Task> failures){}

    public record DailySchedule(LocalTime dailyTime) implements Schedule {
        @Override
        public Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime) {
            LocalDate date = currentTime.toLocalDate();
            if (!dailyTime.isAfter(currentTime.toLocalTime()))
                date = date.plusDays(1);
            return Optional.of(LocalDateTime.of(date, dailyTime));
        }
    }

    public record DurationSchedule(Duration duration) implements Schedule {
        @Override
        public Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime) {
            return Optional.of(currentTime.plus(duration));
        }
    }
}
