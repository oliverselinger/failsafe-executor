package os.failsafe.executor;


import os.failsafe.executor.utils.Database;
import os.failsafe.executor.utils.SystemClock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

class Execution {

    private final Database database;
    private final Task task;
    private final FailsafeExecutor.TaskRegistration taskRegistration;
    private final List<TaskExecutionListener> listeners;
    private final SystemClock systemClock;
    private final TaskRepository taskRepository;

    Execution(Database database, Task task, FailsafeExecutor.TaskRegistration taskRegistration, List<TaskExecutionListener> listeners, SystemClock systemClock, TaskRepository taskRepository) {
        this.database = database;
        this.task = task;
        this.taskRegistration = taskRegistration;
        this.listeners = listeners;
        this.systemClock = systemClock;
        this.taskRepository = taskRepository;
    }

    public String perform() {
        try {
            if (taskRegistration.requiresTransaction()) {
                database.transaction(connection  -> {
                    taskRegistration.transactionalFunction.accept(connection, task.getParameter());
                    taskRepository.delete(connection, task);
                });
            }

            if (taskRegistration.isRegularTask()) {
                taskRegistration.function.accept(task.getParameter());
                taskRepository.delete(task);
            }

            if (taskRegistration.isScheduled()) {
                taskRegistration.function.accept(task.getParameter());

                Optional<LocalDateTime> nextExecutionTime = taskRegistration.schedule.nextExecutionTime(systemClock.now());
                if (nextExecutionTime.isPresent()) {
                    taskRepository.unlock(task, nextExecutionTime.get());
                } else {
                    taskRepository.delete(task);
                }
            }

            notifySuccess();

        } catch (Exception exception) {
            taskRepository.saveFailure(task, exception);

            notifyFailed(exception);
        }

        return task.getId();
    }

    private void notifySuccess() {
        listeners.forEach(l -> l.succeeded(task.getName(), task.getId(), task.getParameter()));
    }

    private void notifyFailed(Exception exception) {
        listeners.forEach(l -> l.failed(task.getName(), task.getId(), task.getParameter(), exception));
    }
}
