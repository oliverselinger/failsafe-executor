package os.failsafe.executor;

import os.failsafe.executor.schedule.OneTimeSchedule;
import os.failsafe.executor.schedule.Schedule;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.DefaultSystemClock;
import os.failsafe.executor.utils.NamedThreadFactory;
import os.failsafe.executor.utils.SystemClock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDateTime;
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

import static os.failsafe.executor.utils.ExecutorServiceUtil.shutdownAndAwaitTermination;

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

    public FailsafeExecutor(DataSource dataSource) {
        this(new DefaultSystemClock(), dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, DEFAULT_INITIAL_DELAY, DEFAULT_POLLING_INTERVAL, DEFAULT_LOCK_TIMEOUT);
    }

    public FailsafeExecutor(SystemClock systemClock, DataSource dataSource, int workerThreadCount, int queueSize, Duration initialDelay, Duration pollingInterval, Duration lockTimeout) {
        if (queueSize < workerThreadCount) {
            throw new IllegalArgumentException("QueueSize must be >= workerThreadCount");
        }

        if (lockTimeout.compareTo(Duration.ofMinutes(5)) < 0) {
            throw new IllegalArgumentException("LockTimeout must be >= 5 minutes");
        }

        this.database = new Database(dataSource);
        this.systemClock = systemClock;
        this.taskRepository = new TaskRepository(database, systemClock);
        this.persistentQueue = new PersistentQueue(taskRepository, systemClock, lockTimeout);
        this.workerPool = new WorkerPool(workerThreadCount, queueSize);
        this.initialDelay = initialDelay;
        this.pollingInterval = pollingInterval;
    }

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
        for (;;) if (executeNextTask() == null) break;
    }

    public void stop() {
        this.workerPool.stop();
        shutdownAndAwaitTermination(executor);
        running.set(false);
    }

    public boolean registerTask(String name, Consumer<String> function) {
        if (tasksByName.containsKey(name)) {
            return false;
        }
        tasksByName.put(name, function);
        return true;
    }

    public String execute(String taskName, String parameter) {
        return database.connect(connection -> execute(connection, taskName, parameter));
    }

    public String execute(Connection connection, String taskName, String parameter) {
        Task taskInstance = new Task(UUID.randomUUID().toString(), parameter, taskName, systemClock.now());
        return enqueue(connection, taskInstance);
    }

    public String schedule(String scheduleName, Schedule schedule) {
        return database.connect(connection -> schedule(connection, scheduleName, schedule));
    }

    public String schedule(Connection connection, String scheduleName, Schedule schedule) {
        if (!scheduleByName.containsKey(scheduleName)) {
            scheduleByName.put(scheduleName, schedule);
        }
        LocalDateTime plannedExecutionTime = schedule.nextExecutionTime(systemClock.now())
                .orElseThrow(() -> new IllegalArgumentException("Schedule must return at least one execution time"));

        Task task = new Task(UUID.randomUUID().toString(), null, scheduleName, plannedExecutionTime);
        return enqueue(connection, task);
    }

    public List<Task> allTasks() {
        return taskRepository.findAll();
    }

    public Optional<Task> task(String String) {
        return Optional.ofNullable(taskRepository.findOne(String));
    }

    public List<Task> failedTasks() {
        return taskRepository.findAllFailedTasks();
    }

    public void subscribe(TaskExecutionListener listener) {
        listeners.add(listener);
    }

    public void unsubscribe(TaskExecutionListener listener) {
        listeners.remove(listener);
    }

    public boolean isLastRunFailed() {
        return lastRunException != null;
    }

    public Exception lastRunException() {
        return lastRunException;
    }

    private String enqueue(Connection connection, Task task) {
        String String = persistentQueue.add(connection, task);
        notifyRegistration(task, String);

        return String;
    }

    private Future<String> executeNextTask() {
        try {
            if (workerPool.allWorkersBusy()) {
                return null;
            }

            Task toExecute = persistentQueue.peekAndLock();
            if (toExecute == null) {
                return null;
            }

            Consumer<String> consumer = tasksByName.get(toExecute.getName());
            //TODO: handle unknown tasks gracefully
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

    private void notifyRegistration(Task task, String String) {
        listeners.forEach(listener -> listener.registered(task.getName(), String, task.getParameter()));
    }

}
