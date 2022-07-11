package os.failsafe.executor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import os.failsafe.executor.db.DbExtension;
import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.TestSystemClock;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_LOCK_TIMEOUT;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_TABLE_NAME;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_WORKER_THREAD_COUNT;

class MultipleNodesShould {
    private final TestSystemClock systemClock = new TestSystemClock();
    private TaskRepository taskRepository;

    private static final Logger log = LoggerFactory.getLogger(MultipleNodesShould.class);

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    DataSource dataSource;
    FailsafeExecutor executorA;
    FailsafeExecutor executorB;
    TaskExecutionListener listenerA;
    TaskExecutionListener listenerB;

    TaskFunction<String> task1;
    TaskFunction<String> task2;

    @BeforeEach
    void init() throws SQLException {
        dataSource = DB_EXTENSION.dataSource();
        systemClock.resetTime();

        executorA = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, 5, Duration.ofMillis(0), Duration.ofMillis(10), DEFAULT_LOCK_TIMEOUT);
        executorB = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, 5, Duration.ofMillis(0), Duration.ofMillis(5), DEFAULT_LOCK_TIMEOUT);
        listenerA = Mockito.mock(TaskExecutionListener.class);
        listenerB = Mockito.mock(TaskExecutionListener.class);

        taskRepository = new TaskRepository(new Database(dataSource), DEFAULT_TABLE_NAME, systemClock);

        executorA.subscribe(listenerA);
        executorB.subscribe(listenerB);

        task1 = s -> {
            log.info("Processed on Worker 1: " + s);
        };
        task2 = s -> {
            log.info("Processed on Worker 2: " + s);
        };
    }

    @AfterEach
    void stop() {
        executorA.stop(15, TimeUnit.SECONDS);
        executorB.stop(15, TimeUnit.SECONDS);
    }

    @Test
    void do_work_sharding() {
        executorA.registerTask("Task1", task1);
        executorB.registerTask("Task1", task2);

        final int total = 40;

        for (int i = 0; i < total; i++) {
            executorA.execute("Task1", "Test" + i);
        }
        executorA.start();
        executorB.start();

        awaitEmptyTaskTable();

        assertListenerOnSucceeded(listenerA, "Task1", 2);
        assertListenerOnSucceeded(listenerB, "Task1", 2);

        assertEquals(total, countFinishedTasks(listenerA) + countFinishedTasks(listenerB));
    }

    @Test
    void run_only_processable_tasks() {
        final int total = 15;
        executorA.registerTask("Task2", task1);
        executorA.registerRemoteTask("Task3");
        executorB.registerTask("Task3", task2);


        for (int i = 0; i < total; i++) {
            executorA.execute("Task2", "Task 2 Test" + i);
            executorA.execute("Task3", "Task 3 Test" + i);
        }

        executorA.start();
        executorB.start();

        awaitEmptyTaskTable();

        assertListenerOnSucceeded(listenerA, "Task2", total);
        assertListenerOnSucceeded(listenerB, "Task3", total);

        assertEquals(2 * total, countFinishedTasks(listenerA) + countFinishedTasks(listenerB));
    }

    private void assertListenerOnSucceeded(TaskExecutionListener listener, String name, int count) {
        verify(listener, timeout((int) TimeUnit.SECONDS.toMillis(1)).atLeast(count)).succeeded(eq(name), any(), any());
    }

    private long countFinishedTasks(TaskExecutionListener l) {
        return Mockito.mockingDetails(l).getInvocations().stream().filter(i -> i.getMethod().getName().equals("succeeded")).count();
    }

    private void awaitEmptyTaskTable() {
        try {
            await().atMost(Duration.ofSeconds(1))
                    .pollInterval(Duration.ofMillis(100))
                    .until(() -> findAllTasks().isEmpty());
        } catch (Exception e) {
            System.err.println("######################");
            System.err.println("Tasks still present:");
            findAllTasks().forEach(task -> System.err.println(task.toString()));
            System.err.println("######################");
        }
    }

    private List<Task> findAllTasks() {
        return taskRepository.findAll(null, null, null, 0, 100);
    }
}
