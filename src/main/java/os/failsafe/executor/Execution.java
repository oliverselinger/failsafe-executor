package os.failsafe.executor;

import os.failsafe.executor.schedule.Schedule;
import os.failsafe.executor.task.PersistentTask;
import os.failsafe.executor.task.Task;
import os.failsafe.executor.task.TaskExecutionListener;
import os.failsafe.executor.task.TaskId;
import os.failsafe.executor.utils.SystemClock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

class Execution {

    private final Task task;
    private final PersistentTask persistentTask;
    private final List<TaskExecutionListener> listeners;
    private final Schedule schedule;
    private final SystemClock systemClock;
    private final PersistentTaskRepository persistentTaskRepository;

    Execution(Task task, PersistentTask persistentTask, List<TaskExecutionListener> listeners, Schedule schedule, SystemClock systemClock, PersistentTaskRepository persistentTaskRepository) {
        this.task = task;
        this.persistentTask = persistentTask;
        this.listeners = listeners;
        this.schedule = schedule;
        this.systemClock = systemClock;
        this.persistentTaskRepository = persistentTaskRepository;
    }

    public TaskId perform() {
        try {
            task.run(persistentTask.getParameter());

            notifySuccess();

            Optional<LocalDateTime> nextExecutionTime = schedule.nextExecutionTime(systemClock.now());
            if (nextExecutionTime.isPresent()) {
                persistentTaskRepository.unlock(persistentTask, nextExecutionTime.get());
            } else {
                persistentTaskRepository.delete(persistentTask);
            }
        } catch (Exception e) {
            persistentTaskRepository.saveFailure(persistentTask, e);

            notifyFailed();
        }

        return persistentTask.getId();
    }

    private void notifySuccess() {
        listeners.forEach(this::notifySuccess);
    }

    private void notifySuccess(TaskExecutionListener listener) {
        listener.succeeded(persistentTask.getName(), persistentTask.getId(), persistentTask.getParameter());
    }

    private void notifyFailed() {
        listeners.forEach(this::notifyFailed);
    }

    private void notifyFailed(TaskExecutionListener listener) {
        listener.failed(persistentTask.getName(), persistentTask.getId(), persistentTask.getParameter());
    }
}
