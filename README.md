# failsafe-executor

[![](https://jitpack.io/v/oliverselinger/failsafe-executor.svg)](https://jitpack.io/#oliverselinger/failsafe-executor)
[![CircleCI](https://circleci.com/gh/oliverselinger/failsafe-executor.svg?style=svg)](https://circleci.com/gh/oliverselinger/failsafe-executor)


**STILL WORK IN PROGRESS**

Persistent executor service for Java that was inspired by the need for a reliable and multi-node compatible execution of processes simpler than BPMN workflow engines provide.

## Features

* **Failsafe** tasks. Requires only one database-table for persistence.
* **Reliable** execution. Guarantees at least once execution of a submitted tasks
* **Multi-node compatible**. Coordination between nodes with optimistic locking
* **Retry-able**. Exceptions are captured. Failed tasks can be retried.
* **Lightweight**. Small code base.
* **No dependencies**.

## Getting started

1. Add the JitPack repository to your build file
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

2. Add the dependency
```xml
<dependency>
    <groupId>com.github.oliverselinger</groupId>
    <artifactId>failsafe-executor</artifactId>
    <version>0.3.0</version>
</dependency>
```

3. Create the table in your database. See [oracle](src/main/resources/oracle.sql) or [postgres](src/main/resources/postgres.sql) or [mysql](src/main/resources/mysql.sql)

3. Instantiate and start the `FailsafeExecutor`, which then will start executing any submitted tasks.

```java
FailsafeExecutor failsafeExecutor = new FailsafeExecutor(dataSource);
failsafeExecutor.start();
```

## Execute Tasks

### Define the execution logic

You need to define the logic that should be executed by the `FailsafeExecutor`. For that, create a `TaskDefinition`. It consists of an arbitrary chosen id that must be
unique application-wide. The actual logic is defined via a consumer that accepts a String as parameter, used for state transfer.
The parameter can be anything but we recommend to use just a single ID that your business logic is able to interpret properly. Avoid using a complex object
(through serialization) since it may lead to complex migration scenarios in case business logic changes.

```java
TaskDefinition taskDefinition = TaskDefinitions.of("UniqueTaskName", parameter -> log.info("Hello {}", parameter));
failsafeExecutor.defineTask(taskDefinition);
```

### Create and execute a task

To execute a task create a new instance by using the `TaskDefinition`. There you can define the parameter.
The task is then executed some time in the future by passing it to the `FailsafeExecutor`.

```java
Task task = taskDefinition.newTask(" world!");
TaskId taskId = failsafeExecutor.execute(task);
```

## Monitoring the execution

The result of execution of a task can be observed by subscribing a listener either at the `TaskDefinition`:

```java
TaskExecutionListener executionListener = new TaskExecutionListener() { ... };
taskDefinition.subscribe(executionListener);
```

or globally at the `FailsafeExecutor`:

```java
failsafeExecutor.subscribe(executionListener);
```

Listeners subscribed at a `TaskDefinition` get called only if a corresponding task gets executed.
A global listener gets called each time an execution is performed.

The listener gets called at the end of the execution in an at least once manner.

## Task failures

Any exceptions occurring during the execution of a task are captured. The exception's message and stacktrace are saved to the task. The task itself is marked as failed.
Thus the `FailsafeExecutor` does not execute the task anymore. To find failed tasks use the following:

```java
List<FailedTask> failedTasks = failsafeExecutor.failedTasks();
```

Two options are offered to handle a failed task. Either retry it:

```java
failedTask.retry();
```

Or cancel it:

```java
failedTask.cancel();
```

Cancel deletes the task from database.

## Monitoring

The `FailsafeExecutor` provides a health check through two methods. One that returns if last run of `FailsafeExecutor` was successful.

```java
failsafeExecutor.isLastRunFailed();
```

And another method to retrieve the exception of the last run.

```java
Exception e = failsafeExecutor.lastRunException();
```

## Shutdown of the executor

It is important to shutdown the FailsafeExecutor properly by calling the `stop` method. E.g. create a shutdownHook

```java
Runtime.getRuntime().addShutdownHook(new Thread() {
    @Override
    public void run() {
        failsafeExecutor.stop();
    }
});
```

## Configuration

The `FailsafeExecutor` can be created using the all-args constructor. The following options are configurable:

| Option  | Type | Default | Description |
| ------------- | ---- | ---- | ------------- |
| `systemClock` | `SystemClock`  | LocalDateTime.now() | Clock to retrieve the current time. |
| `workerThreadCount` | `int`  | 5  | Number of threads executing tasks. |
| `queueSize` | `int`  |  2 * `<worker-thread-count>`  | Maximum number of tasks to lock by the `FailsafeExecutor` at the same time. |
| `initialDelay` | `Duration`  |  10 sec  | The time to delay first execution to fetch tasks of the 'FailsafeExecutor'. |
| `pollingInterval` | `Duration`  |  5 sec  | How often the 'FailsafeExecutor' checks for tasks to execute. |

## FAQ

#### Can method `execute` take part in a Spring-managed transaction?

Yes. Wrap your `dataSource` object with a `TransactionAwareDataSourceProxy` before passing it to FailsafeExecutor's constructor. The proxy adds awareness of Spring-managed transactions.

```java
@Bean(destroyMethod = "stop")
public FailsafeExecutor failsafeExecutor(DataSource dataSource) {
    FailsafeExecutor failsafeExecutor = new FailsafeExecutor(new TransactionAwareDataSourceProxy(dataSource));
    failsafeExecutor.start();
    return failsafeExecutor;
}
```