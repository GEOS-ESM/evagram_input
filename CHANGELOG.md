# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Enforced the reference to the readers metadata table when inserting diagnostics
- Added helper method for parsing datetime strings into a datetime object given the provided formats in the filename
- Added return/error messages after a completed or terminated workflow run with a basic summary of the amount of diagnostics added
- Added configuration files to setup a test database during workflow runs using the PGPASS file
- Added schema testing to validate SQL operations made by the input tool
- Added the first version of the input tool module that opens database sessions for Swell workflow tasks
- Added `CODEOWNERS` file

### Changed

- Modified run_task method to search diagnostics under the updated file structure

### Fixed

- Return message bug that incorrectly displays the connection as open when an unhandled exception has occurred

### Removed

### Deprecated
