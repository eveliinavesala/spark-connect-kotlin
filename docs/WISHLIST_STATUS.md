# Developer Wishlist: Status & Roadmap

This document tracks our progress against the original developer's wishlist for a modern, idiomatic Kotlin Spark API.

## Core Wishlist & Current Status

### 1. Completed & Verified

This section covers the wishlist items that are now successfully implemented and tested in our project.

| Wish | Status & Implementation Details |
| :--- | :--- |
| **Focus on Spark Connect 4+** | **DONE.** The project is built entirely on the `spark-connect-client-jvm:4.0.0` dependency. This foundational decision has guided all subsequent architectural choices. |
| **Kotlin data classes should be supported by default** | **DONE.** This is the core achievement of our reflection-based adapter. Our `toDataFrame()` and `toKotlinList()` functions provide robust, default support for idiomatic Kotlin data classes without requiring no-arg constructors. |
| **Encoders should be inferred with `reified` types** | **DONE (in spirit).** We have fulfilled the user-experience goal of this wish. Our `toDataFrame()` and `toKotlinList<MyClass>()` functions use `inline reified` generics to provide a seamless, type-safe experience without the user needing to manually pass `KClass` objects. This replaces the original, incompatible `encoder<T>()` mechanism. |
| **It should be easy to get started** | **DONE.** The public API is minimal and intuitive. A user only needs to learn two primary functions (`toDataFrame`, `toKotlinList`) to handle the most common data conversion tasks. |
| **Additional classes support** | **DONE.** Our adapter has been proven to support a wide range of complex types beyond simple data classes, including: `List`, `Set`, `Map`, `Enum`, arbitrarily **nested data classes**, polymorphic **sealed class hierarchies**, and **`kotlinx.datetime`** types (`LocalDate`, `Instant`). |
| **UDFs** | **DONE.** We have a working and tested pattern for using Kotlin lambdas as UDFs. This involves creating a "fat JAR" and using `spark.addArtifact()` to send it to the server, as demonstrated in `SparkETLTest`. |
| **Kotlin Notebook support should work** | **DONE (for local development).** We have successfully established a pragmatic workflow for in-IDE Kotlin Notebook development by configuring the IDE to use the project's `main` classpath. This proves the library's interactive capabilities. |

### 2. To-Do & Future Work

This section covers wishlist items that are not yet implemented and represent the next major features to tackle.

| Wish | Status & Plan |
| :--- | :--- |
| **Structured Streaming support** | **PENDING.** We have not yet touched the Structured Streaming APIs. **Plan:** This is a major feature that would require a separate research and development effort, likely starting with a `streaming_etl` test package to investigate how our reflection adapter behaves with streaming DataFrames. |
| **User-Defined Types (UDTs)** | **PENDING.** This is a very advanced Spark feature for representing custom types within Spark's own type system. **Plan:** This would be a major research effort. Our current reflection adapter serves a similar purpose on the client side, but integrating with the formal `UserDefinedType` API would be a significant undertaking. |

### 3. Needs More Robust Testing & Refinement

This section covers features that are working but could be improved or have untested edge cases, particularly concerning the developer experience for a *consumer* of our library.

| Area | Status & Refinement Plan |
| :--- | :--- |
| **Notebook Experience for Consumers** | The current in-IDE workflow is excellent for *us*, but a user of our published library would have a different experience. **Plan:** Create documentation explaining how a consumer would add our library to their own `build.gradle.kts` and use it in their notebook. Test this workflow to ensure it's smooth. |
| **Performance Benchmarking** | Our reflection-based adapter is convenient, but we have not measured its performance against the native (but failing) `Encoders.bean()` or other potential serialization methods. **Plan:** Create a dedicated test suite with large datasets to benchmark the performance of `toDataFrame` and `toKotlinList`. |
| **Error Handling & Diagnostics** | Our error messages are functional (e.g., `IllegalArgumentException` for nulls), but they could be more user-friendly. **Plan:** Review our `error()` calls and exceptions. Can we provide more context to the user about *why* a failure occurred (e.g., which field in which class failed to serialize)? |
