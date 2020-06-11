package os.failsafe.executor;

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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_LOCK_TIMEOUT;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_QUEUE_SIZE;
import static os.failsafe.executor.FailsafeExecutor.DEFAULT_WORKER_THREAD_COUNT;

public class MultipleNodesShould {

    private static final Logger log = LoggerFactory.getLogger(MultipleNodesShould.class);

    private final TestSystemClock systemClock = new TestSystemClock();
    private TaskRepository taskRepository;

    @RegisterExtension
    static final DbExtension DB_EXTENSION = new DbExtension();

    DataSource dataSource;
    FailsafeExecutor executorA;
    FailsafeExecutor executorB;
    TaskExecutionListener listenerA;
    TaskExecutionListener listenerB;

    Consumer<String> task1;


    @BeforeEach
    void init() {
        dataSource = DB_EXTENSION.dataSource();
        systemClock.resetTime();

        executorA = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        executorB = new FailsafeExecutor(systemClock, dataSource, DEFAULT_WORKER_THREAD_COUNT, DEFAULT_QUEUE_SIZE, Duration.ofMillis(0), Duration.ofMillis(1), DEFAULT_LOCK_TIMEOUT);
        listenerA = Mockito.mock(TaskExecutionListener.class);
        listenerB = Mockito.mock(TaskExecutionListener.class);

        taskRepository = new TaskRepository(new Database(dataSource), systemClock);

        executorA.subscribe(listenerA);
        executorB.subscribe(listenerB);

        task1  = s -> {};

        executorA.registerTask("Task1", task1);
        executorB.registerTask("Task1", task1);
        executorA.registerTask("Task2", task1);
    }

    @Test
    void test_work_sharding() {
        final int total = 30;

        for (int i=0; i < total; i++) {
            executorA.execute("Task1", "Test"+i);
        }
        executorA.start();
        executorB.start();

        awaitEmptyTaskTable();

        assertListenerOnSucceeded(listenerA, "Task1",1);
        assertListenerOnSucceeded(listenerB, "Task1",1);

        assertEquals(total, countFinishedTasks(listenerA) + countFinishedTasks(listenerB));
    }

    private void assertListenerOnSucceeded(TaskExecutionListener listener, String name, int count) {
        verify(listener, timeout((int) TimeUnit.SECONDS.toMillis(1)).atLeast(count)).succeeded(eq(name), any(), any());
    }

    private long countFinishedTasks(TaskExecutionListener l) {
        return Mockito.mockingDetails(l).getInvocations().stream().filter(i -> i.getMethod().getName().equals("succeeded")).count();
    }

    private void awaitEmptyTaskTable() {
        while(taskRepository.findAll().size() > 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
