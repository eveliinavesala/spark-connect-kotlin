# Investigation Log: Nested & Sealed Types

This document tracks the specific investigation into supporting nested data classes and sealed hierarchies within the pragmatic `DataFrame` approach.

## Objective

To extend the pragmatic `DataFrame` adapter to correctly serialize and deserialize arbitrarily nested data classes.

## Current Status: **Unsolved**

The current implementation of the pragmatic adapter **does not** support nested data classes. This is a known limitation and the subject of ongoing research.

## Initial Failing Test Case

The problem is isolated with the `PragmaticNestedTest`, which attempts a simple round-trip of a `SimpleUser` object containing a nested `SimpleProfile` object. This test is currently located in the `wip_nested_types` package and is marked as `@Disabled`.

```kotlin
data class SimpleProfile(
    val email: String,
    val website: String?
)

data class SimpleUser(
    val id: Long,
    val username: String,
    val profile: SimpleProfile
)
```

The failure is a `java.lang.ClassCastException` during serialization, proving the conversion logic is incomplete.

## Attempted Solutions & Root Cause Analysis

### Attempt 1: Simple Recursive `Row` Creation

- **Approach:** The `convertKotlinValueToSpark` function was modified to check `if (value::class.isData)`. If true, it would recursively call a helper to convert the nested object into a `GenericRowWithSchema`.
- **Failure:** This resulted in a `Receiver type 'KProperty1<out Any, *>' contains out projection` compilation error.
- **Root Cause `[IMPL-SER-3]`:** This is a fundamental limitation of Kotlin's reflection API. When reflecting on a generic `Any` object, the compiler cannot guarantee that the object instance is a valid receiver for a property (`KProperty1`) obtained from that object's class. The `out` projection correctly makes this code fail at compile-time because it is not type-safe.

### Attempt 2: The Map-based Approach

- **Approach:** The entire serialization logic was changed to convert the Kotlin object graph into a nested `List` of `Map<String, Any?>`, with the intent of using a (presumed) `spark.createDataFrame(List<Map>)` method.
- **Failure:** This resulted in a `None of the following functions can be called with the arguments supplied` compilation error.
- **Root Cause:** The presumed `createDataFrame(List<Map>)` API does not exist in the Spark Connect `SparkSession`. This approach was based on a flawed assumption about the available API surface.

## Next Steps

The path forward requires a deeper understanding of Spark's serialization internals.

1.  **Deep Dive into Spark Source Code:** A manual review of `ArrowSerializer.scala` is required to understand exactly what object type it expects when it encounters a `StructType` in the schema. Our assumption that it requires a `GenericRowWithSchema` may be correct, but our method of constructing it is flawed.
2.  **Re-evaluate the Reflection Strategy:** A new, type-safe method for recursively building `Row` objects must be designed, likely avoiding the `KProperty1<out Any, *>` trap.

This problem is now parked until this further research is complete.
