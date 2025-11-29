# Failure Analysis Log

This document records the key failures encountered during the development of the Kotlin-Spark interoperability layer, the root causes, and the lessons learned. Its purpose is to prevent repeating these mistakes.

## 1. Build & Configuration Failures

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[BUILD-1]` | `Unresolved reference: isData` | The `kotlin-reflect` library was not on the **compile classpath**. | The `kotlin-reflect` dependency **must** be an `implementation` dependency. |
| `[BUILD-2]` | `Unresolved reference: junit`, `Unresolved reference: Testcontainers` | The `org.testcontainers:junit-jupiter` dependency was removed. | This artifact contains the core `@Testcontainers` and `@Container` annotations and is required. |
| `[BUILD-3]` | `ClassNotFoundException: ...PragmaticUDFTest` | The Spark server did not have our project's compiled test classes on its classpath. | Any custom code for server-side execution must be sent as an artifact. |
| `[BUILD-4]` | `ClassNotFoundException: ...GenericContainer` | The JAR artifact sent to the server only contained our project's code, not its dependencies. | The artifact must be a **"fat JAR"** that includes all runtime dependencies. |

## 2. Implementation Failures (`DataFrameApproach.kt`)

### 2.1 Serialization Failures (Kotlin -> Spark)

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[IMPL-SER-1]` | `ClassCastException: ...HashMap cannot be cast to scala.collection.Map` | We converted Kotlin collections to **Java** collections, but the Arrow serializer expected **Scala** collections. | **When serializing, convert Kotlin collections to Scala collections** (`Seq`, `scala.collection.Map`) using `CollectionConverters`. |
| `[IMPL-SER-2]` | `ClassCastException: ...UserProfile cannot be cast to org.apache.spark.sql.Row` | Our serialization logic was not recursive for nested data classes. | The serialization logic must be **fully recursive**. When a data class is encountered, it must be converted into a `GenericRowWithSchema`. |
| `[IMPL-SER-3]` | `Receiver type ... contains out projection` (Compiler Error) | A flawed recursive pattern using `prop.get(obj)` on a generic `KProperty1<out Any, *>` was not type-safe. | Avoid this specific reflection pattern. A safer way is to check `value::class.isData` inside a `when` block where the type is concrete. |

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
