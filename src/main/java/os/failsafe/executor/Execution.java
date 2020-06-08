package os.failsafe.executor;


import os.failsafe.executor.schedule.Schedule;
import os.failsafe.executor.utils.SystemClock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

class Execution {

    private final Task task;
    private final Runnable runnable;
    private final List<TaskExecutionListener> listeners;
    private final Schedule schedule;
    private final SystemClock systemClock;
    private final PersistentTaskRepository taskRepository;

    Execution(Task task, Runnable runnable, List<TaskExecutionListener> listeners, Schedule schedule, SystemClock systemClock, PersistentTaskRepository taskRepository) {
        this.task = task;
        this.runnable = runnable;
        this.listeners = listeners;
        this.schedule = schedule;
        this.systemClock = systemClock;
        this.taskRepository = taskRepository;
    }

    public String perform() {
        try {
            runnable.run();

            notifySuccess();

            Optional<LocalDateTime> nextExecutionTime = schedule.nextExecutionTime(systemClock.now());
            if (nextExecutionTime.isPresent()) {
                taskRepository.unlock(task, nextExecutionTime.get());
            } else {
                taskRepository.delete(task);
            }
        } catch (Exception e) {
            taskRepository.saveFailure(task, e);

            notifyFailed();
        }

        return task.getId();
    }

    private void notifySuccess() {
        listeners.forEach(l -> l.succeeded(task.getName(), task.getId(), task.getParameter()));
    }

    private void notifyFailed() {
        listeners.forEach(l -> l.failed(task.getName(), task.getId(), task.getParameter()));
    }
}
