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

## 4. Build System & Gradle Failures (Recent Debugging Cycle)

| ID | Symptom (Error Message) | Root Cause | The Lesson / Correct Pattern |
| :--- | :--- | :--- | :--- |
| `[BUILD-GRADLE-1]` | `Cannot infer type for type parameter 'S'. Specify it explicitly.` | Misunderstanding of Gradle Kotlin DSL for `sourceSets.create()`. Attempted to force type inference where explicit declaration was needed. | Explicitly declare type parameters for generic Gradle DSL functions (e.g., `sourceSets.create<SourceSet>("app")`). |
| `[BUILD-GRADLE-2]` | `Error resolving plugin [id: 'org.gradle.application', version: '8.5', apply: false]` | Incorrectly adding a version number to a core Gradle plugin (`application`). | Core Gradle plugins do not require (and do not allow) version numbers in the `plugins` block. |
| `[BUILD-GRADLE-3]` | `Error resolving plugin [id: 'org.gradle.application', apply: false] > Plugin 'org.gradle.application' is a core Gradle plugin, which is already on the classpath. Requesting it with the 'apply false' option is a no-op.` | Misunderstanding of how to declare core plugins in a multi-project `build.gradle.kts`. | Core plugins should not be declared in the root `build.gradle.kts` with `apply false`. They are available by default and should be applied directly in subproject `build.gradle.kts` files where needed. |
| `[BUILD-GRADLE-4]` | `Unresolved reference: toDF` / `toList` in test files after refactoring. | My own chaotic refactoring and failure to consistently update all call sites and imports after moving code. | Maintain strict discipline during refactoring. Always update all call sites and imports immediately and consistently. |
| `[BUILD-GRADLE-5]` | `Daemon compilation failed: Could not connect to Kotlin compile daemon` | Incorrectly adding `kotlin-reflect` to `kotlinCompilerClasspath` using `kotlinCompilerPluginClasspath(kotlin("reflect"))` without fully understanding its implications. | This configuration is for compiler *plugins*, not for adding dependencies to the compiler's own runtime classpath. It broke the compiler's ability to start. |
| `[BUILD-GRADLE-6]` | `Unresolved reference: isData` (after `[BUILD-GRADLE-5]` fix) | The `kotlinCompilerClasspath` constraint (`constraints { add("kotlinCompilerClasspath", "org.jetbrains.kotlin:kotlin-reflect:1.9.24") }`) was insufficient to make `isData` available to the compiler. | While the constraint correctly influenced version resolution, it did not ensure the `isData` extension was correctly loaded by the compiler in all contexts. The ultimate solution was found to be an incorrect import in the source code itself (`[BUILD-1]`). |
