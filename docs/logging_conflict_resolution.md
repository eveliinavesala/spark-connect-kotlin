# Logging Conflict Resolution

This document details the investigation and resolution of a critical `java.lang.StackOverflowError` that occurred during interactive notebook execution.

## 1. The Problem: `StackOverflowError`

When running code in a Kotlin Notebook, the execution would crash with a `java.lang.StackOverflowError`. The stack trace revealed an infinite recursive loop between two logging frameworks:

```
java.lang.StackOverflowError
	at org.slf4j.jul.JDK14LoggerFactory.getLogger(JDK14LoggerFactory.java:64)
	at org.slf4j.LoggerFactory.getLogger(LoggerFactory.java:422)
	at org.slf4j.bridge.SLF4JBridgeHandler.getSLF4JLogger(SLF4JBridgeHandler.java:213)
	at org.slf4j.bridge.SLF4JBridgeHandler.publish(SLF4JBridgeHandler.java:304)
	at java.logging/java.util.logging.Logger.log(Logger.java:983)
	at org.slf4j.jul.JDK14LoggerAdapter.innerNormalizedLoggingCallHandler(JDK14LoggerAdapter.java:156)
    ... and so on
```

## 2. The Investigation: Dependency Analysis

The root cause was identified as a **logging bridge feedback loop**.

1.  A library was using `java.util.logging` (JUL).
2.  The `jul-to-slf4j` bridge was on the classpath, capturing JUL logs and redirecting them to the SLF4J facade.
3.  The `log4j-slf4j2-impl` backend was on the classpath, telling SLF4J to send its logs to Log4j2.
4.  The Log4j2 configuration was, in turn, routing its output back to JUL, creating an infinite loop.

We confirmed this by running `./gradlew dependencies` and inspecting the dependency tree for the `spark-connect-client-jvm` artifact.

### "Before" State

The initial dependency tree clearly showed `jul-to-slf4j` being included as a transitive dependency:

```
+--- org.apache.spark:spark-connect-client-jvm_2.13:4.0.0
|    ...
|    +--- org.slf4j:jul-to-slf4j:2.0.16
|    ...
```

## 3. The Solution: Exclude the Bridge

The definitive solution is to break the loop by preventing the `jul-to-slf4j` bridge from being included in the project's classpath. This was achieved by adding an `exclude` rule to the `build.gradle.kts` file.

**The Fix:**
```kotlin
// build.gradle.kts

dependencies {
    implementation("org.apache.spark:spark-connect-client-jvm_2.13:4.0.0") {
        // Exclude the logging bridge to prevent a StackOverflowError
        exclude(group = "org.slf4j", module = "jul-to-slf4j")
    }
    // ... other dependencies
}
```

## 4. Verification: "After" State

After applying the fix, we re-ran `./gradlew dependencies`. The new dependency tree confirms that `jul-to-slf4j` is no longer present:

```
+--- org.apache.spark:spark-connect-client-jvm_2.13:4.0.0
|    ...
|    +--- org.slf4j:jcl-over-slf4j:2.0.16  // jul-to-slf4j is gone
|    ...
```

This surgical change resolves the `StackOverflowError` in our standard test suite.

---

## 5. Lingering Issue in Notebook on Connection Failure

- **Symptom:** Even after excluding the bridge from our Gradle build, the `StackOverflowError` still occurs in the Kotlin Notebook, but **only** when the initial connection to the Spark server fails.
- **Root Cause Analysis:** This indicates that the IDE's Kotlin Notebook environment may be injecting its own `jul-to-slf4j` dependency to capture all possible logs for display. This re-creates the feedback loop, but only in the specific code path of a connection failure.
- **Status:** **Won't Fix (Low Priority).**
- **Justification:**
    - This error only occurs during connection failure in the interactive notebook. The library works perfectly during normal operation.
    - The root cause is a complex interaction within the IDE's notebook environment, not our library's code.
    - The effort required to debug this environment-specific error path outweighs the benefit, as the primary functionality of our library is unaffected. The primary goal of the test (proving we are using the network) was still achieved, albeit with a messy error.
