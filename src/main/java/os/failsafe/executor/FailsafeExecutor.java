package os.failsafe.executor;

import os.failsafe.executor.schedule.OneTimeSchedule;
import os.failsafe.executor.schedule.Schedule;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.DefaultSystemClock;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class FailsafeExecutor {

    public static final int DEFAULT_WORKER_THREAD_COUNT = 5;
    public static final int DEFAULT_QUEUE_SIZE = DEFAULT_WORKER_THREAD_COUNT * 4;
    public static final Duration DEFAULT_INITIAL_DELAY = Duration.ofSeconds(10);
    public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5);
    public static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofMinutes(12);

    private final Map<String, Consumer<String>> tasksByName = new ConcurrentHashMap<>();
    private final Map<String, Schedule> scheduleByName = new ConcurrentHashMap<>();
    private final List<TaskExecutionListener> listeners = new CopyOnWriteArrayList<>();

    private final OneTimeSchedule oneTimeSchedule = new OneTimeSchedule();

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Failsafe-Executor-"));
    private final PersistentQueue persistentQueue;
    private final WorkerPool workerPool;
    private final TaskRepository taskRepository;
    private final Duration initialDelay;
    private final Duration pollingInterval;
    private final Database database;
    private final SystemClock systemClock;

    private volatile Exception lastRunException;
    private AtomicBoolean running = new AtomicBoolean();

    public FailsafeExecutor(DataSource dataSource) throws SQLException {
        this(new DefaultSystemClock(), dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, DEFAULT_INITIAL_DELAY, DEFAULT_POLLING_INTERVAL, DEFAULT_LOCK_TIMEOUT);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout) throws SQLException {
        if (queueSize < workerThreadCount) {
            throw new IllegalArgumentException("QueueSize must be >= workerThreadCount");
        }

        if (lockTimeout.compareTo(Duration.ofMinutes(5)) < 0) {
            throw new IllegalArgumentException("LockTimeout must be >= 5 minutes");
        }

        this.database = new Database(dataSource);
        this.systemClock = () -> systemClock.now().truncatedTo(ChronoUnit.MILLIS);
        this.taskRepository = new TaskRepository(database, systemClock);
        this.persistentQueue = new PersistentQueue(taskRepository, systemClock, lockTimeout);
        this.workerPool = new WorkerPool(workerThreadCount, queueSize);
        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
    }

    /**
     * Start execution of any submitted tasks.
     */
    public void start() {
        boolean shouldStart = running.compareAndSet(false, true);
        if (!shouldStart) {
            return;
        }

        executor.scheduleWithFixedDelay(
                this::executeNextTasks,
                initialDelay.toMillis(), pollingInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void executeNextTasks() {
        for (; ; ) {
            if (executeNextTask() == null) {
                break;
            }

            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
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
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            // ignore
        }

        workerPool.stop(timeout, timeUnit);

        running.set(false);
    }

    /**
     * Registers the given function under the provided name.
     *
     * <p>Make sure your function is idempotent, since it gets executed at least once per task execution.</p>
     *
     * @param name     unique name of the task that should be persisted
     * @param function the function that should be assigned to the unique name, accepting a parameter.
     * @return true if the initial registration of the task with the unique name has been successfully completed, false if the task has been persisted already
     */
    public boolean registerTask(String name, Consumer<String> function) {
        if (tasksByName.containsKey(name)) {
            return false;
        }
        tasksByName.put(name, function);
        return true;
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
     * @return taskId
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
     * @return taskId
     */
    public String execute(Connection connection, String taskId, String taskName, String parameter) {
        Task taskInstance = new Task(taskId, parameter, taskName, systemClock.now());
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
     * @return taskId
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
     * @return taskId
     */
    public String defer(Connection connection, String taskId, String taskName, String parameter, LocalDateTime plannedExecutionTime) {
        Task taskInstance = new Task(taskId, parameter, taskName, plannedExecutionTime);
        return enqueue(connection, taskInstance);
    }

    /**
     * Schedules the execution of the provided runnable. The task is then executed at the planned execution times
     * defined by the schedule.
     *
     * <p>With a {@link Schedule} you get a recurring execution as long as Schedule returns a {@link LocalDateTime}.</p>
     *
     * <p>Make sure your runnable is idempotent, since it gets executed at least once per scheduled execution time.</p>
     *
     * @param taskName the name of the task that should be executed
     * @param schedule the schedule that defines the planned execution times
     * @param runnable the runnable
     * @return taskId
     */
    public String schedule(String taskName, Schedule schedule, Runnable runnable) {
        return database.connect(connection -> schedule(connection, taskName, schedule, runnable));
    }

    /**
     * Schedules the execution of the provided runnable. The task is then executed at the planned execution times
     * defined by the schedule.
     *
     * <p>With a {@link Schedule} you get a recurring execution as long as Schedule returns a {@link LocalDateTime}.</p>
     *
     * <p>Make sure your runnable is idempotent, since it gets executed at least once per scheduled execution time.</p>
     *
     * <p>The provided connection is used for persisting the task in the database. Neither commit
     * nor rollback is triggered. The control of the transactional behavior is completely up to the caller.</p>
     *
     * @param connection the JDBC connection used to persist the task in the database
     * @param taskName   the name of the task that should be executed
     * @param schedule   the schedule that defines the planned execution times
     * @return taskId
     */
    public String schedule(Connection connection, String taskName, Schedule schedule, Runnable runnable) {
        if (!scheduleByName.containsKey(taskName)) {
            scheduleByName.put(taskName, schedule);
        }

        if (!tasksByName.containsKey(taskName)) {
            tasksByName.put(taskName, ignore -> runnable.run());
        }

        LocalDateTime plannedExecutionTime = schedule.nextExecutionTime(systemClock.now())
                .orElseThrow(() -> new IllegalArgumentException("Schedule must return at least one execution time"));

        Task task = new Task(taskName, null, taskName, plannedExecutionTime);
        return enqueue(connection, task);
    }

    /**
     * Returns all persisted tasks.
     *
     * @return list of all persisted tasks
     */
    public List<Task> allTasks() {
        return taskRepository.findAll();
    }

    /**
     * Returns a single task.
     *
     * @param taskId the id of the task
     * @return the task
     */
    public Optional<Task> task(String taskId) {
        return Optional.ofNullable(taskRepository.findOne(taskId));
    }

    /**
     * Returns all tasks that failed during execution.
     *
     * <p>The failure details are found in the {@link ExecutionFailure} of a task.</p>
     *
     * @return list of all failed tasks
     */
    public List<Task> failedTasks() {
        return taskRepository.findAllFailedTasks();
    }

    public boolean retry(Task failedTask) {
        if (failedTask.isRetryable()) {
            listeners.forEach(listener -> listener.retrying(failedTask.getName(), failedTask.getId(), failedTask.getParameter()));
            taskRepository.deleteFailure(failedTask);
            return true;
        }

        return false;
    }

    public boolean cancel(Task failedTask) {
        if (failedTask.isCancelable()) {
            taskRepository.delete(failedTask);
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

    private String enqueue(Connection connection, Task task) {
        notifyPersisting(task, task.getId());
        return persistentQueue.add(connection, task);
    }

    private Future<String> executeNextTask() {
        try {
            if (workerPool.allWorkersBusy()) {
                return null;
            }

            Task toExecute = persistentQueue.peekAndLock(tasksByName.keySet());
            if (toExecute == null) {
                return null;
            }

            Consumer<String> consumer = tasksByName.get(toExecute.getName());
            Schedule schedule = scheduleByName.getOrDefault(toExecute.getName(), oneTimeSchedule);
            Execution execution = new Execution(toExecute, () -> consumer.accept(toExecute.getParameter()), listeners, schedule, systemClock, taskRepository);
            Future<String> future = workerPool.execute(toExecute.getId(), execution::perform);

            clearException();

            return future;
        } catch (Exception e) {
            storeException(e);
        }

        return null;
    }

    private void storeException(Exception e) {
        lastRunException = e;
    }

    private void clearException() {
        lastRunException = null;
    }

    private void notifyPersisting(Task task, String taskId) {
        listeners.forEach(listener -> listener.persisting(task.getName(), taskId, task.getParameter()));
    }
}
