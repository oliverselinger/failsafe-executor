# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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