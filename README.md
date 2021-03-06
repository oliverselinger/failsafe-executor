# failsafe-executor

[![](https://img.shields.io/maven-central/v/com.github.oliverselinger/failsafe-executor)]()
[![](https://jitpack.io/v/oliverselinger/failsafe-executor.svg)](https://jitpack.io/#oliverselinger/failsafe-executor)
[![CircleCI](https://circleci.com/gh/oliverselinger/failsafe-executor.svg?style=svg)](https://circleci.com/gh/oliverselinger/failsafe-executor)
[![codecov](https://codecov.io/gh/oliverselinger/failsafe-executor/branch/master/graph/badge.svg)](https://codecov.io/gh/oliverselinger/failsafe-executor)

Persistent executor service for Java that was inspired by the need for a reliable and multi-node compatible execution of processes.

## Features

* **Failsafe** tasks. Requires only one database-table for persistence.
* **Reliable** execution. Guarantees at least once execution of a submitted task.
* **Multi-node compatible**. Coordination between nodes with optimistic locking.
* **Retry-able**. Exceptions are captured. Failed tasks can be retried.
* **Routing** tasks amongst different nodes. An executor only picks up registered tasks.
* **Lightweight**. Small code base.
* **No dependencies**.
* **No reflection**.

## Getting started

1.  Add the dependency
```xml
<dependency>
    <groupId>com.github.oliverselinger</groupId>
    <artifactId>failsafe-executor</artifactId>
    <version>1.1.0</version>
</dependency>
```

3. Create the table in your database. See [oracle](src/main/resources/oracle.sql) or [postgres](src/main/resources/postgres.sql) or [mysql](src/main/resources/mysql.sql)

3. Instantiate and start the `FailsafeExecutor`, which then will start executing any submitted tasks.

```java
FailsafeExecutor failsafeExecutor = new FailsafeExecutor(dataSource);
failsafeExecutor.start();
```

## Execute Tasks

Execution of a task requires two steps:

### Register the task

First, register the task on startup of your application with a consumer that accepts a single input argument for state transfer. Give the task a unique name.

```java
failsafeExecutor.registerTask("TaskName", param -> {
    ... // your business logic
});
```

An executor only picks up tasks which have been registered with a function. If you desire task execution on a different node, you only need to register the remote task's name via the`registerRemoteTask` method.
This allows for simple **routing**, based on task names amongst different nodes.

Make sure your business logic is **idempotent**, since it gets executed at least once per task execution.

As parameter, we recommend to use only a single ID that your business logic is able to interpret properly. Avoid using complex objects
(through serialization) since it may lead to complex migration scenarios in case your object and your business logic changes.

### Execute the task

Pass your task's name and your parameter to FailsafeExecutor's `execute` method. The task gets persisted and is then executed at some time in the future.

```java
String taskId = failsafeExecutor.execute("TaskName", parameter);
```

Optionally you can provide a taskId that is used as unique constraint in the database. On conflict (task with this id already exists in database) insertion is simply skipped.
In this case no exception will be thrown and method returns gracefully.

```java
String taskId = failsafeExecutor.execute("UniqueTaskId", "TaskName", parameter);
```

#### Defer execution

You can plan a **one-time execution** in future with method `defer`.

```java
String taskId = failsafeExecutor.defer("TaskName", parameter, plannedExecutionTime);
```

## Schedule Tasks

You can schedule a task's execution. Pass your task's name, your `Schedule` and your runnable to FailsafeExecutor's `schedule` method.

```java
String taskId = failsafeExecutor.schedule("TaskName", schedule, () -> {
... // your business logic
});
```

For a **recurring execution** let your `Schedule` always return the next planned time for execution. For example see [DailySchedule](src/main/java/os/failsafe/executor/schedule/DailySchedule.java).

As before, make sure your business logic is **idempotent**, since it gets executed at least once per scheduled execution.

## Task failures

Any exceptions occurring during the execution of a task are captured. The exception's message and stacktrace are saved to the task. The task itself is marked as failed.
Thus the `FailsafeExecutor` does not execute the task anymore. To find failed tasks use the following:

```java
List<Task> failedTasks = failsafeExecutor.failedTasks();
```

Two options are offered to handle a failed task. Either retry it:

```java
failsafeExecutor.retry(failedTask);
```

Or cancel it:

```java
failsafeExecutor.cancel(failedTask);
```

Cancel deletes the task from database.

## Monitoring the execution

The result of an execution can be observed by subscribing a listener at the `FailsafeExecutor`:

```java
failsafeExecutor.subscribe(executionListener);
```

The `persisting` method gets called before a task gets persisted in database. At the end of the execution, depending on the outcome either `succeeded` or `failed` is called.
A retry of a failed task causes a call of method `retrying` before failure state gets deleted in database.

## Health check

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
        failsafeExecutor.stop(15, TimeUnit.SECONDS);
    }
});
```

## Configuration

The `FailsafeExecutor` can be created using the all-args constructor. The following options are configurable:

| Option  | Type | Default | Description |
| ------------- | ---- | ---- | ------------- |
| `systemClock` | `SystemClock` | LocalDateTime.now() | Clock to retrieve the current time. |
| `workerThreadCount` | `int` | 5 | Number of threads executing tasks. |
| `queueSize` | `int`  |  4 * `<worker-thread-count>` | Maximum number of tasks to lock by the `FailsafeExecutor` at the same time. |
| `initialDelay` | `Duration` |  10 sec | The time to delay first execution to fetch tasks of the `FailsafeExecutor`. |
| `pollingInterval` | `Duration` |  5 sec | How often the `FailsafeExecutor` checks for tasks to execute. |
| `lockTimeout` | `Duration` |  13 min | If a task is locked for execution, but is not deleted nor updated due to e.g. a system crash, it will again be considered for execution after this timeout. Minimum lockTimeout is 5 min. |

**Note:** Consider the lockTimeout must be longer than `(queueSize / workerThreadCount) * task-max-execution-time`. We expect the maximum execution time of a task as 3 min.
With the default configuration you get `(4*5 / 5) * 3 min = 12 min`. Therefore default lockTimeout is 13 min.

## Testability

In class `FailsafeExecutorTestUtility` you find some static methods to support you with your integration tests. Just wrap your business logic with `awaitAllTasks`. This registers a listener before executing your business logic.
With it all created tasks during the execution of your business logic are registered. After execution finishes, a barrier blocks the calling thread until all tasks got executed.

```java
awaitAllTasks(failsafeExecutor, () -> {
    ... // your business logic
}, failedTasks -> {
    ... // e.g. let your unit test case fail immediately
});
```

After all tasks finished execution, failed tasks are collected and are passed to the callback consumer function. E.g. with that call you can let your test case fail immediately.

In some cases you don't want to wait for certain tasks to finish, like deferred ones. You can ignore certain tasks by passing a `NoWaitPredicate` as parameter.

## FAQ

#### How is an at-least-once execution guaranteed?

First, each task gets persisted into database before it's considered for execution. After that, the `FailsafeExecutor` tries to reserve the next task based on creation date by setting a lock timestamp in the database. Concurrent
access by several FailsafeExecutors is controlled by applying optimistic locking. Only if the lock operation succeeds, the task is submitted for execution to the FailsafeExecutor's worker pool. In case,
the `FailsafeExecutor` is not able to execute all his locked tasks, e.g. due to a system crash, a predefined lock timeout guarantees that a task will again be considered for execution by other FailsafeExecutors which may be running
on different nodes.

#### Are tasks executed in the insertion order?

No. Basically, the `FailsafeExecutor` orders tasks by creation date for locking. However then locked tasks are executed by a pool of threads. So execution order can not be guaranteed. Furthermore more randomness is applied
if the `FailsafeExecutor` is running on multiple nodes.

#### Is there any retry mechanism?

No. For that, you can implement it yourself inside of a tasks runnable or consumer function or utilize a library, e.g. [resilience4j](https://github.com/resilience4j/resilience4j)

#### Can method `execute` and `schedule` take part in a Spring-managed transaction?

Yes. Wrap your `dataSource` object with a `TransactionAwareDataSourceProxy` before passing it to FailsafeExecutor's constructor. The proxy adds awareness of Spring-managed transactions.

```java
@Bean(destroyMethod = "stop")
public FailsafeExecutor failsafeExecutor(DataSource dataSource) {
    FailsafeExecutor failsafeExecutor = new FailsafeExecutor(new TransactionAwareDataSourceProxy(dataSource));
    failsafeExecutor.start();
    return failsafeExecutor;
}
```

#### Is it running in production?

Yes.

#### What Java versions are supported?

Requires Java 8+.

#### Are the artifacts available in other repositories than Maven Central Repository?

Yes, in the JitPack repository. For usage add the JitPack repository to your build file:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```
