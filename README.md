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
    <version>0.1.0</version>
</dependency>
```

3. Create the table in your database. See [oracle](src/main/resources/oracle.sql) or [postgres](src/main/resources/postgres.sql) or [mysql](src/main/resources/mysql.sql)

3. Instantiate and start the FailsafeExecutor, which then will start executing any submitted tasks.

```java
FailsafeExecutor failsafeExecutor = new FailsafeExecutor(dataSource);
failsafeExecutor.start();
```

## Execute Tasks

### Define the execution logic

You need to define the program that should be executed by the FailsafeExecutor. For that, create a taskDefinition. It consists of an arbitrary chosen id that must be
unique application-wide. The actual logic is defined via a consumer that accepts a String as parameter, used for state transfer.
The parameter can be anything but we recommend to use just a single ID that your business logic is able to interpret properly. Avoid using a complex object
(through serialization) since it may lead to complex migration scenarios in case business logic changes.

```java
TaskDefinition taskDefinition = TaskDefinitions.of("UniqueTaskName", parameter -> log.info("Hello {}", parameter));
failsafeExecutor.defineTask(taskDefinition);
```

### Create and execute a task

To execute a task create a new instance by using the TaskDefinition. There you can define the parameter.
The task is then executed some time in the future by passing it to the FailsafeExecutor.

```java
Task task = taskDefinition.newTask(" world!");
PersistentTask persistentTask = failsafeExecutor.execute(task);
```

## Monitoring the execution

The result of execution of a task can be observed by subscribing a listener at the TaskDefinition.

```java
TaskExecutionListener executionListener = new TaskExecutionListener() { ... };
taskDefinition.subscribe(executionListener);
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
