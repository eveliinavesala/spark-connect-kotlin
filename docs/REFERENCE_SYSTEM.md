# Project Reference System

This document defines the semantic reference system used to track failures and their corresponding solutions throughout this research project.

## Format

Each unique issue is assigned a reference ID with the format: `[CATEGORY]-[ID]`

### Categories

- **`[BUILD]`**: Issues related to the Gradle build (`build.gradle.kts`), dependencies, plugins, or configuration.
- **`[IMPL-SER]`**: Issues related to the pragmatic implementation's **serialization** logic (converting Kotlin objects **to** Spark `Row`s).
- **`[IMPL-DES]`**: Issues related to the pragmatic implementation's **deserialization** logic (converting Spark `Row`s **back to** Kotlin objects).
- **`[ENV]`**: Issues related to the test environment, including Docker, Testcontainers, and file pathing.

### ID

- A simple integer, incremented for each new issue within a category.

### Purpose

This system creates a "chain of evidence." When a failure is documented in `failure_analysis_log.md` with an ID (e.g., `[IMPL-DES-1]`), the corresponding successful pattern that fixed it will be tagged with the same ID in `success_analysis_log.md`.

This allows for clear, searchable, and unambiguous tracking of our research progress.
