# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2020-11-11
### Added
- Enhancement of `AwaitableTaskExecutionListener` to handle registrations of multiple tasks even outside of a transaction. 
This fixes race conditions that could occur in cases where a task registers and finishes before the next task registers.
- This CHANGELOG file.

## [1.0.0] - 2020-09-29
### Added
- All desired features are tested and working. First general available version. 