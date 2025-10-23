package os.failsafe.executor;

import os.failsafe.executor.schedule.Schedule;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * DistributedTaskQueue - Multi-node, DB-backed task executor for FAILSAFE_TASK schema.
 * Supports scheduling, retries, heartbeats, observers, restart, and error handling.
 */
public class DistributedTaskQueue {

    private static final Logger log = Logger.getLogger(DistributedTaskQueue.class.getName());

    private final SystemClock systemClock;
    private final Duration initialDelay;
    private final Duration pollingInterval;
    private final Duration lockTimeout;
    final Duration heartbeatInterval;
    private final String nodeId;
    private final int maxWorkers;
    private final int queueSize;
    private final DataSource dataSource;
    private final boolean isOracleDb;

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

    public DistributedTaskQueue(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout, String nodeId) throws SQLException {
        this.systemClock = systemClock;
        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
        this.nodeId = nodeId;
        this.maxWorkers = workerThreadCount;
        this.lockTimeout = lockTimeout;
        this.heartbeatInterval = Duration.ofMillis(lockTimeout.toMillis() / 3);
        this.queueSize = queueSize;
        this.dataSource = dataSource;
        validateDatabase();
        try (Connection connection = dataSource.getConnection()) {
            var databaseName = connection.getMetaData().getDatabaseProductName();
            this.isOracleDb = databaseName.equalsIgnoreCase("Oracle");
        }
    }

    // -------------------- LIFECYCLE --------------------

    public synchronized void start() {
        if (running) {
            DistributedTaskQueue.log.warning("DistributedTaskQueue already running.");
            return;
        }
        running = true;

        this.executor = Executors.newFixedThreadPool(maxWorkers);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleWithFixedDelay(this::pollAndSubmit, initialDelay.toMillis(), pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
        heartbeatExecutor.scheduleWithFixedDelay(this::extendLeasesBatch, initialDelay.toMillis(), heartbeatInterval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("DistributedTaskQueue started.");
    }

    public synchronized void shutdown() {
        if (!running) return;
        running = false;
        log.info("Shutting down DistributedTaskQueue...");
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
        log.info("DistributedTaskQueue stopped.");
    }

    public boolean isRunning() {
        return running && !executor.isShutdown() && !scheduler.isShutdown() && !heartbeatExecutor.isShutdown();
    }

    // -------------------- REGISTRATION --------------------

    public void registerErrorHandler(Consumer<Throwable> handler) {
        this.uncaughtErrorHandler = handler;
    }

    public void registerTask(String name, TaskFunction<String> function) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(function, null)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    public void registerTask(String name, TransactionalTaskFunction<String> function) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(null, function)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    public void registerRemoteTask(String name) {
        if (taskRegistry.putIfAbsent(name, new TaskRegistration(null, null)) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }
    }

    public void schedule(String name, Schedule schedule, ThrowingRunnable runnable) throws SQLException {
        registerTask(name, ignore -> runnable.run());
        if (scheduleRegistry.putIfAbsent(name, schedule) != null) {
            throw new IllegalArgumentException("Task '%s' is already registered".formatted(name));
        }

        LocalDateTime plannedExecutionTime = schedule.nextExecutionTime(systemClock.now())
                .orElseThrow(() -> new IllegalArgumentException("Schedule must return at least one execution time"));

        try (Connection connection = dataSource.getConnection()) {
            execute(connection, name, name, null, plannedExecutionTime);
        }
    }

    public void subscribe(TaskExecutionListener listener) {
        listeners.add(listener);
    }

    public void unsubscribe(TaskExecutionListener listener) {
        listeners.remove(listener);
    }

    // -------------------- TASK MANAGEMENT --------------------

    public void execute(Connection conn, String id, String taskName, String parameter) throws SQLException {
        execute(conn, id, taskName, parameter, systemClock.now());
    }

    public void execute(Connection conn, String id, String taskName, String parameter, LocalDateTime plannedExecutionTime) throws SQLException {
        notifyPersisting(taskName, id, parameter);
        String sql = """
                INSERT INTO FAILSAFE_TASK (ID, NAME, PARAMETER, PLANNED_EXECUTION_TIME, CREATED_DATE)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, taskName);
            stmt.setString(3, parameter);
            stmt.setTimestamp(4, Timestamp.valueOf(plannedExecutionTime));
            stmt.executeUpdate();
        }
        log.info("Task submitted: " + taskName + " (" + id + ")");
    }

    public boolean retry(String id) throws SQLException {
        String sql = """
                UPDATE FAILSAFE_TASK
                   SET FAIL_TIME = NULL,
                       LOCK_TIME = NULL,
                       NODE_ID = NULL,
                       RETRY_COUNT = RETRY_COUNT + 1,
                       EXCEPTION_MESSAGE = NULL,
                       STACK_TRACE = NULL,
                       PLANNED_EXECUTION_TIME = ?
                 WHERE ID = ? AND FAIL_TIME IS NOT NULL
                """;

        notifyRetrying(new Task(id, null, null)); // FIXME

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(true);

            stmt.setTimestamp(1, Timestamp.valueOf(systemClock.now()));
            stmt.setString(2, id);
            int updated = stmt.executeUpdate();

            return updated == 1;
        }
    }

    public void cancel(String taskId) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            delete(conn, taskId);
        }
    }

    Task findOne(String id) throws SQLException, IOException {
        String sql = "SELECT * FROM FAILSAFE_TASK WHERE ID=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return Task.from(rs);
            }
        }
        return null;
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
        Set<String> registeredTaskNames = taskRegistry.keySet().stream().sorted().collect(Collectors.toCollection(LinkedHashSet::new));
        String processableTasksPlaceHolder = IntStream.range(0, registeredTaskNames.size()).mapToObj(s -> "?").collect(Collectors.joining(","));

        List<Task> tasks = new ArrayList<>();
        String sql = """
                SELECT ID, NAME, PARAMETER, LOCK_TIME, NODE_ID
                  FROM FAILSAFE_TASK
                 WHERE (LOCK_TIME IS NULL OR LOCK_TIME < ?)
                   AND FAIL_TIME IS NULL
                   AND PLANNED_EXECUTION_TIME <= ?
                   AND NAME IN (%s) 
                   %s
                 FOR UPDATE SKIP LOCKED
                """;

        var skipLocked = "ORDER BY PLANNED_EXECUTION_TIME LIMIT ?";
        if (isOracleDb) {
            skipLocked = "AND ROWNUM <= ? ORDER BY PLANNED_EXECUTION_TIME";
        }

        sql = sql.formatted(processableTasksPlaceHolder, skipLocked);

        String updateSql = "UPDATE FAILSAFE_TASK SET LOCK_TIME = ?, NODE_ID = ? WHERE ID = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql);
             PreparedStatement updateStmt = connection.prepareStatement(updateSql)) {
            connection.setAutoCommit(false);

            LocalDateTime now = systemClock.now();
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
        log.info("Executing task: " + task.name + " (" + task.id + ")");
        try {
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
                    Optional<LocalDateTime> next = schedule.nextExecutionTime(LocalDateTime.now());
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

            LocalDateTime now = systemClock.now();
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
            stmt.setTimestamp(1, Timestamp.valueOf(systemClock.now()));
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

    private void notifyPersisting(String name, String id, String param) {
        listeners.forEach(l -> safeInvoke(() -> l.persisting(name, id, param)));
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

    private record TaskRegistration(TaskFunction<String> function,
                                    TransactionalTaskFunction<String> trxConsumingFunction) {
    }

    public interface SystemClock {
        LocalDateTime now();
    }

    public interface Schedule {
        Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime);
    }

    public interface TaskExecutionListener {
        void persisting(String name, String id, String parameter);
        void retrying(String name, String id, String parameter);
        void succeeded(String name, String id, String parameter);
        void failed(String name, String id, String parameter, Exception exception);
    }

    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public interface TaskFunction<T> {
        void accept(T param) throws Exception;
    }

    public interface TransactionalTaskFunction<T> {
        void accept(Connection connection, T param) throws Exception;
    }

    record Task(String id, String name, String parameter, String nodeId, LocalDateTime creationTime,
                LocalDateTime plannedExecutionTime, LocalDateTime lockTime, ExecutionFailure executionFailure,
                int retryCount) {

        public Task(String id, String name, String parameter) {
            this(id, name, parameter, null, null, null, null, null, 0);
        }

        public static Task from(ResultSet rs) throws SQLException, IOException {
            Timestamp lockTime = rs.getTimestamp("LOCK_TIME");

            return new Task(
                    rs.getString("ID"),
                    rs.getString("NAME"), rs.getString("PARAMETER"),
                    rs.getString("NODE_ID"),
                    rs.getTimestamp("CREATED_DATE").toLocalDateTime(),
                    rs.getTimestamp("PLANNED_EXECUTION_TIME").toLocalDateTime(),
                    lockTime != null ? lockTime.toLocalDateTime() : null,
                    mapToExecutionFailure(rs),
                    rs.getInt("RETRY_COUNT"));
        }

        public static ExecutionFailure mapToExecutionFailure(ResultSet rs) throws SQLException, IOException {
            Timestamp failTime = rs.getTimestamp("FAIL_TIME");
            if (rs.wasNull()) {
                return null;
            }
            String exceptionMessage = rs.getString("EXCEPTION_MESSAGE");
            StringWriter writer = new StringWriter();
            rs.getCharacterStream("STACK_TRACE").transferTo(writer);
            String stackTrace = writer.toString();

            return new ExecutionFailure(failTime.toLocalDateTime(), exceptionMessage, stackTrace);
        }
    }

    record ExecutionFailure(LocalDateTime failTime, String exceptionMessage, String stackTrace) {
    }

    public static TaskExecutionListener waitForTasks(Runnable taskProducer) throws InterruptedException {
        AtomicInteger active = new AtomicInteger();
        Object lock = new Object();

        TaskExecutionListener listener = new TaskExecutionListener() {
            public void persisting(String n, String i, String p) {
                synchronized (lock) { active.incrementAndGet(); }
            }
            public void retrying(String n, String i, String p) {}
            public void succeeded(String n, String i, String p) { done(); }
            public void failed(String n, String i, String p, Exception e) { done(); }
            private void done() {
                synchronized (lock) {
                    if (active.decrementAndGet() == 0) lock.notifyAll();
                }
            }
        };

        taskProducer.run();

        synchronized (lock) {
            while (active.get() > 0) lock.wait();
        }
        return listener;
    }

    public class DailySchedule implements Schedule {
        private final LocalTime dailyTime;

        public DailySchedule(LocalTime dailyTime) {
            this.dailyTime = dailyTime;
        }

        @Override
        public Optional<LocalDateTime> nextExecutionTime(LocalDateTime currentTime) {
            LocalDate date = currentTime.toLocalDate();
            if (!dailyTime.isAfter(currentTime.toLocalTime()))
                date = date.plusDays(1);
            return Optional.of(LocalDateTime.of(date, dailyTime));
        }
    }
}
