# Developer Wishlist: Status & Roadmap

This document tracks our progress against the original developer's wishlist for a modern, idiomatic Kotlin Spark API.

## Core Wishlist & Current Status

### 1. Completed & Verified

This section covers the wishlist items that are now successfully implemented, tested, and ready for production use.

| Wish | Status & Implementation Details |
| :--- | :--- |
| **Focus on Spark Connect 4+** | **DONE.** The project is built entirely on the `spark-connect-client-jvm:4.0.0` dependency. This foundational decision has guided all subsequent architectural choices. |
| **Kotlin data classes should be supported by default** | **DONE.** This is the core achievement of our reflection-based adapter. Our `toDataFrame()` and `toKotlinList()` functions provide robust, default support for idiomatic Kotlin data classes without requiring no-arg constructors. |
| **Encoders should be inferred with `reified` types** | **DONE (in spirit).** We have fulfilled the user-experience goal of this wish. Our `toDataFrame()` and `toKotlinList<MyClass>()` functions use `inline reified` generics to provide a seamless, type-safe experience without the user needing to manually pass `KClass` objects. |
| **It should be easy to get started** | **DONE.** The public API is minimal and intuitive. A user only needs to learn two primary functions (`toDataFrame`, `toKotlinList`) and a few DSL extensions to be productive. |
| **Additional classes support** | **DONE.** Our adapter has been proven to support a wide range of complex types beyond simple data classes, including: `List`, `Set`, `Map`, `Enum`, arbitrarily **nested data classes**, polymorphic **sealed class hierarchies**, **`kotlinx.datetime`** types, and **`java.time`** types. |
| **UDFs** | **DONE.** We have a working and tested pattern for using Kotlin lambdas as UDFs. This involves creating a "fat JAR" that includes test classes and using `spark.addArtifact()` to send it to the server. This pattern is proven to work in `PragmaticUDFTest`. |
| **User-Defined Types (UDTs)** | **DONE.** We have successfully implemented and tested support for UDTs. The solution involves annotating a custom class with `@SQLUserDefinedType` and updating our reflection engine to recognize and handle the serialization/deserialization via the UDT class. |
| **Value Classes** | **DONE.** The reflection engine was extended to support Kotlin `value class`, correctly "unwrapping" them to their underlying primitive type for Spark and "wrapping" them back upon retrieval. |
| **Kotlin Notebook support should work** | **DONE.** By building a standard JVM library, our API works out-of-the-box in any Kotlin Notebook environment (Jupyter, Datalore). Users simply add the library as a dependency. Our `addArtifact` strategy for UDFs also ensures that functions defined in notebook cells can be correctly executed on the remote Spark cluster, enabling a seamless interactive workflow. |

### 2. To-Do & Future Work

This section covers wishlist items that are not yet implemented and represent the next major features to tackle.

| Wish | Status & Plan |
| :--- | :--- |
| **Structured Streaming support** | **PENDING.** We have not yet written specific tests for Structured Streaming. However, since the Streaming API largely reuses the `DataFrame` API, our existing library is expected to be highly compatible. **Plan:** Create a `streaming_etl` test package to verify that `readStream`, `writeStream`, and our `toKotlinList` adapter (on micro-batches) work as expected. |
| **Performance Benchmarking** | **PENDING.** Our reflection-based adapter is convenient, but we have not measured its performance against other potential serialization methods. **Plan:** Create a dedicated test suite with large datasets to benchmark the performance of `toDataFrame` and `toKotlinList`, and publish the results so users understand the trade-offs. |

### 3. Needs More Robust Testing & Refinement

This section covers features that are working but could be improved or have untested edge cases.

| Area | Status & Refinement Plan |
| :--- | :--- |
| **Error Handling & Diagnostics** | Our error messages are functional (e.g., `IllegalArgumentException` for nulls), but they could be more user-friendly. **Plan:** Review our `error()` calls and exceptions. Can we provide more context to the user about *why* a failure occurred (e.g., which field in which class failed to serialize)? |
| **Library Packaging & Publishing** | The project is now structured as a proper library in the `integration_pack` package. **Plan:** The next step is to configure the `build.gradle.kts` file to publish the `integration_pack` as a standalone Maven artifact, making it easy for other projects to consume. |
