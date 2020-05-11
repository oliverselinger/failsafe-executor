# failsafe-executor

**STILL WORK IN PROGRESS**

Persistent executor service for Java that was inspired by the need for a reliable and multi-node compatible execution of processes simpler than BPMN workflow engines provide.

## Features

* **Persistent** tasks. Requires only two database-tables for persistence.
* **Retry-able**. Exceptions are captured. Failed tasks can be retried.
* **Multi-node compatible**. Guarantees at least once execution of a submitted task
* **Lightweight**. Small code base.
* **Minimal dependencies**. (slf4j)

## Getting started

1. Add maven dependency
```xml
<dependency>
    <groupId>os.failsafe.executor</groupId>
    <artifactId>failsafe-scheduler</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
2. Create the tables in your database. See [sql](failsafe-executor/src/main/resources/tables.sql)

3. Instantiate and start the FailsafeExecutor, which then will start executing any defined tasks.

```java
FailsafeExecutor failsafeExecutor = new FailsafeExecutor(systemClock, dataSource, 5, 1, 1);
failsafeExecutor.start();
```

## Execute Tasks

### Define a task

The creation of a task requires an arbitrary chosen id that must be unique within the application. The actual task is defined via a consumer that accepts a String for state transfer.
State transfer via complex objects through serialization is not recommended since it may lead to complex migration scenarios in case business logic changes.
We recommend to us just an id that your business logic is able to interpret properly.

```java
Task task = Tasks.of("HelloWorldTask", parameter -> log.info("Hello {}", parameter));
failsafeExecutor.register(task);
```

### Execute a task

The execution of a task is done by creation of an instance of the task that defines the parameter. The instance is then executed some time in the future by passing it to the FailsafeExecutor.

```java
Task.Instance instance = task.instance(" world!");
String taskInstanceId = failsafeExecutor.execute(instance);
```

## Monitoring the execution

The end of an execution of a task instance can be observed by subscribing a ExecutionEndedListener at the FailsafeExecutor.

```java
Task.ExecutionEndedListener executionEndedListener = new Task.ExecutionEndedListener() { ... };
task.subscribe(executionEndedListener);
```

The listener get called at the end of the execution in an at least once manner.

## Shutdown of the executor

It is important to shutdown the FailsafeExecutor properly by calling the shutdown method. E.g. create a shutdownHook

```java
Runtime.getRuntime().addShutdownHook(new Thread() {
    @Override
    public void run() {
        failsafeExecutor.stop();
    }
});
```