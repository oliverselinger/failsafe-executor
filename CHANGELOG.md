# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.1] - 2025-08-26
### Fixed
- Fix jitpack build

## [2.2.0] - 2025-08-25
### Added
- Restart method for FailsafeExecutor to recover from unexpected termination
- Enhanced logging system for better observability
- TestContainers support for database testing

### Changed
- Updated dependencies in pom.xml
- Migrated from CircleCI to GitHub Actions for CI/CD
- Build with JDK 17 in GitHub workflow

### Fixed
- Error handling improvements for better resilience
- MariaDB schema issues

## [2.1.1] - 2024-06-03
### Fixed
- Fix findAll tasks contains filter criteria

## [2.1.0] - 2024-05-28
### Added
- Add findAll tasks method with multiple new filter criteria

## [2.0.2] - 2022-08-17
### Changed
- Handle stack traces as large string with JDBC's `setCharacterStream` and `getCharacterStream` methods. On Oracle, stack traces having >= 4000 chars caused problems when handled as regular String. 
 
## [2.0.1] - 2022-07-15
### Changed
- Call `e.printStackTrace()` in `storeException(Exception e)` for better visibility

## [2.0.0] - 2022-07-11
### Added
- Validation of JDBC driver's ability to return the effected row count for batched statements. Issue was raised with MariaDB JDBC driver version > 3 which returns SUCCESS_NO_INFO instead. For more details go to the FAQs!
- Methods to retrieve current count of tasks
- Ability to query tasks by different criteria and sort the result set in different ways

### Changed  
- Update JDBC drivers and database test container
- Streamline select queries to retrieve tasks
- Rename method `task` to `findOne`
- Rename method `allTasks` to `findAll`
  
### Removed
- API method `registerTask(String name, Schedule schedule, TaskFunction<String> function)`. No use case found for this function.

## [1.4.0] - 2021-12-19
### Added
- Heartbeat scheduler that updates the lock time on tasks. This should avoid takeover of long-running tasks by another instance.
- Metrics collector `FailsafeExecutorMetricsCollector` to gain insights about persistence, failure and finishing rates and counts.

## [1.3.3] - 2021-11-02
### Added
- Ability to create tasks which get recurringly executed based on a schedule (given during registration) which then receives a parameter for execution

### Changed
- Construct SQL statements in constructor during initialization

## [1.3.2] - 2021-10-28
### Fixed
- Make `Observer` interface of `PersistentQueue` visible to the outside world
- This version should work with H2 database again. Versions 1.3.0 and 1.3.1 do not. But I don't know why. Moved sql statement creation back into methods. Removed reRun threshold and simplified execution logic.

## [1.3.1] - 2021-10-04
### Added
- Support for pageable query methods (offset + limit)
- Limit default query methods to max of 100 results

## [1.3.0] - 2021-09-12
### Added
- Register transactional task functions. Those task functions receive, apart from the parameter, a connection object with an active transaction. This transaction is then used
  to remove the task entry from the table after the execution of the task function ends successfully.
- A custom table name can be set via parameter in constructor.
- Index for created_date in sql scripts to make select query for next tasks fast (ordering)
- Support to record other failures in FailsafeExecutor's context. 
  It can be useful to record exceptions that are thrown not within a failsafe task but in regular synchronous program execution.
  So you can make other exceptions visible and utilize the FailsafeExecutor's retry mechanism.
- Method to register a persistent queue observer to make behavior and select query results visible. 
- Rerun threshold can be configured via parameter in constructor. Determines if FailsafeExecutor should immediately try to lock more tasks after current select and lock run. In the default setting threshold value is 5. That means if there are more than 5 entries free in the queue after a select and lock run, FailsafeExecutor does not wait for the next polling interval. Instead, it immediately reruns trying to lock more tasks.
  
### Changed
- Improvement (performance/pressure on resources): The limit of the select query to get the next tasks for execution is set dynamically based on the value of the spare space in the queue.
- Improvement (performance/pressure on resources): The update queries to lock the next tasks for execution are getting executed as batchUpdate.

### Fixed
- Fixed PK definition in SQL scripts

## [1.2.0] - 2021-08-06
### Added
- Make table name configurable via FailsafeExecutor's constructor.
- Add support for MariaDB

## [1.1.0] - 2021-06-25
### Added
- Fail on enqueuing of a task via 'execute' or 'defer' with a task's name that is not registered.
  This avoids situation where you wait for a task to complete but nothing happens because no task got registered under this name.
- In order to still be able to route tasks amongst different nodes the method 'registerRemoteTask' is added. 
  It only takes a task's name without a function. This further on, allows to enqueue tasks without getting these executed on the same node. 

## [1.0.1] - 2020-11-11
### Added
- Enhancement of `AwaitableTaskExecutionListener` to handle registrations of multiple tasks even outside of a transaction. 
  This fixes race conditions that could occur in cases where a task registers and finishes before the next task registers.
- This CHANGELOG file.

## [1.0.0] - 2020-09-29
### Added
- All desired features are tested and working. First general available version. 