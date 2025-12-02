# Known Issues

This document tracks known, non-critical issues related to the development environment and third-party library interactions.

## 1. `StackOverflowError` in Notebooks on Connection Failure

- **Symptom:** When running code in an in-IDE Kotlin Notebook, if the Spark Connect server is not running, the cell that initiates the Spark connection fails with a `java.lang.StackOverflowError` instead of a clean `Connection refused` error.
- **Root Cause:** This is caused by a logging feedback loop. The Spark client's error handling path for a connection failure appears to generate `java.util.logging` messages. A logging bridge on the notebook's classpath (`jul-to-slf4j`) redirects these to SLF4J, which then routes them back to `java.util.logging` via its Log4j2 backend, creating an infinite loop.
- **Status:** **Won't Fix (Low Priority).**
- **Justification:**
    - This error only occurs during connection failure. The library works perfectly during normal operation.
    - The root cause is a complex interaction within the IDE's notebook environment and Spark's transitive logging dependencies.
    - The effort required to debug and resolve this low-level logging issue outweighs the benefit, as it does not affect the core functionality of our library. The primary goal of the test (proving we are using the network) was still achieved, albeit with a messy error.
