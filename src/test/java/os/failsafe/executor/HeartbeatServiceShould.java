package os.failsafe.executor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import os.failsafe.executor.utils.TestSystemClock;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class HeartbeatServiceShould {

    private final TestSystemClock systemClock = new TestSystemClock();
    private TaskRepository taskRepository;
    private HeartbeatService heartbeatService;
    private Duration heartbeatInterval;
    private FailsafeExecutor failsafeExecutor;

    @BeforeEach
    void init() {
        taskRepository = Mockito.mock(TaskRepository.class);
        heartbeatInterval = Duration.ofMinutes(1);
        failsafeExecutor = Mockito.mock(FailsafeExecutor.class);
        heartbeatService = new HeartbeatService(heartbeatInterval, systemClock, taskRepository);
    }

    @Test
    void not_throw_exception_if_no_tasks_are_registered() {
        assertDoesNotThrow(() -> heartbeatService.heartbeat());

        verify(taskRepository, never()).updateLockTime(any());
        verifyNoError();
    }

    @Test
    void doNothingIfLockIsNotOutdated() {
        systemClock.fixedTime(LocalDateTime.of(2020, 11, 12, 10, 0));

        Task task = createTask();
        heartbeatService.register(task);

        heartbeatService.heartbeat();

        verify(taskRepository, never()).updateLockTime(any());
        verifyNoError();
    }

    @Test
    void updateLockTimeIfLockIsOutdatedAndStopOnUnregister() {
        doAnswer(ans ->
                Collections.singletonList(createTask())
        ).when(taskRepository).updateLockTime(any());

        LocalDateTime now = LocalDateTime.of(2020, 11, 12, 10, 0);
        systemClock.fixedTime(now);

        Task task = createTask();
        heartbeatService.register(task);

        systemClock.fixedTime(now.plus(heartbeatInterval).plusSeconds(1));

        heartbeatService.heartbeat();

        verify(taskRepository, times(1)).updateLockTime(Collections.singletonList(task));
        verifyNoError();

        systemClock.timeTravelBy(Duration.ofSeconds(5));
        heartbeatService.heartbeat(); // lock is not outdated yet

        verifyNoMoreInteractions(taskRepository);
        verifyNoError();
    }

    @Test
    void stopUpdatingForUnregisteredTask() {
        LocalDateTime now = LocalDateTime.of(2020, 11, 12, 10, 0);
        systemClock.fixedTime(now);

        Task task = createTask();
        heartbeatService.register(task);

        systemClock.fixedTime(now.plus(heartbeatInterval).plusSeconds(1));

        heartbeatService.unregister(task);
        heartbeatService.heartbeat();

        verifyNoMoreInteractions(taskRepository);
        verifyNoError();
    }

    private void verifyNoError() {
        verify(failsafeExecutor, never()).storeException(any());
    }

    private Task createTask() {
        return new Task("30e3e24f-fd17-42a5-a24a-9e85a6b44085", "Test", "param", systemClock.now(), systemClock.now(), systemClock.now(), null, 0, 0L);
    }
}
