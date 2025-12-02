# Failure Analysis Log

This document records the key failures encountered during the development of the Kotlin-Spark interoperability layer, the root causes, and the lessons learned. Its purpose is to prevent repeating these mistakes.

## 1. Build & Configuration Failures

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[BUILD-1]` | `Unresolved reference: isData` | **[SOLVED]** This was not a build failure. It was caused by an incorrect `import kotlin.reflect.full.isData`. | The `isData` property is a built-in member of `KClass` and requires no import from `.full`. The `kotlin-reflect` dependency is still required on the `implementation` classpath. |
| `[BUILD-2]` | `Unresolved reference: junit`, `Unresolved reference: Testcontainers` | The `org.testcontainers:junit-jupiter` dependency was removed. | This artifact contains the core `@Testcontainers` and `@Container` annotations and is required. |
| `[BUILD-3]` | `ClassNotFoundException: ...PragmaticUDFTest` | The Spark server did not have our project's compiled test classes on its classpath. | Any custom code for server-side execution must be sent as an artifact. |
| `[BUILD-4]` | `ClassNotFoundException: ...GenericContainer` | The JAR artifact sent to the server only contained our project's code, not its dependencies. | The artifact must be a **"fat JAR"** that includes all runtime dependencies. |

## 2. Implementation Failures (`DataFrameApproach.kt`)

### 2.1 Serialization Failures (Kotlin -> Spark)

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[IMPL-SER-1]` | `ClassCastException: ...HashMap cannot be cast to scala.collection.Map` | We converted Kotlin collections to **Java** collections, but the Arrow serializer expected **Scala** collections. | **When serializing, convert Kotlin collections to Scala collections** (`Seq`, `scala.collection.Map`) using `CollectionConverters`. |
| `[IMPL-SER-2]` | `ClassCastException: ...UserProfile cannot be cast to org.apache.spark.sql.Row` | **[SOLVED]** Our serialization logic was not recursive for nested data classes. | The serialization logic must be **fully recursive**. The final implementation uses a type-safe generic helper function to achieve this. |
| `[IMPL-SER-3]` | `Receiver type ... contains out projection` (Compiler Error) | **[SOLVED]** A flawed recursive pattern using `prop.get(obj)` on a generic `KProperty1<out Any, *>` was not type-safe. | The solution was to create a `private fun <T : Any> convertSpecificObjectToRow(obj: T)` helper. This preserves the specific type `T`, making the call to `prop.get(obj)` provably type-safe. |

### 2.2 Deserialization Failures (Spark -> Kotlin)

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[IMPL-DES-1]` | `IllegalArgumentException: argument type mismatch` | **1.** The `Row` from Spark contained a `scala.collection.Seq`. **2.** The `Seq` contained `org.apache.spark.unsafe.types.UTF8String` elements. | Deserialization must be a **two-stage conversion**: first convert the outer Scala collection to Java/Kotlin, then recursively convert the *elements* inside, handling types like `UTF8String`. |
| `[IMPL-DES-2]` | `NullPointerException` (when expecting `IllegalArgumentException`) | Our code did not explicitly check for `null` values being passed to non-nullable primitive constructor parameters. | Always "fail fast". Before calling the constructor, explicitly check for `null` on non-nullable parameters and throw a descriptive `IllegalArgumentException`. |
| `[IMPL-DES-3]` | `Unresolved reference: forEach` on a `ScalaMap` | We tried to use a Kotlin extension function directly on a `scala.collection.Map`. | **Always convert Scala collections to Java collections first** using `CollectionConverters.asJava()` before manipulating them with standard Kotlin functions. |

## 3. Environment & Runtime Failures

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[ENV-1]` | `AnalysisException: [PATH_NOT_FOUND] file:/data/...` | The Spark server, running in a container, resolves relative paths from its filesystem root (`/`), not its working directory. | Mount data volumes to an **absolute path** (e.g., `/data`) in the container and use that same absolute path in the Spark code. |
| `[ENV-2]` | `java.lang.IllegalAccessException: class ... cannot access a member of class sun.util.calendar.ZoneInfo` | **[SOLVED]** The Java Platform Module System (JPMS) in modern JVMs (9+) prevents reflection on internal JDK APIs. Spark's `SparkDateTimeUtils` was attempting to access the encapsulated `sun.util.calendar.ZoneInfo` class. | The solution is to use the `--add-opens` JVM argument in the Gradle build to explicitly open the required package for Spark: `--add-opens=java.base/sun.util.calendar=ALL-UNNAMED`. |
| `[ENV-3]` | `java.lang.StackOverflowError` in notebook on connection failure | **[WON'T FIX]** A logging feedback loop between the IDE's injected `jul-to-slf4j` bridge and Spark's `log4j2` backend, which only manifests on connection failure. | This is a low-priority issue specific to the interactive environment's error path. The core functionality is unaffected. The fix (`exclude 'jul-to-slf4j'`) was successful in tests but the issue persists in the notebook, confirming it's an environment problem. |
