# Investigation Log: Nested & Sealed Types (RESOLVED)

This document details the full investigation into supporting nested data classes and sealed hierarchies within the reflection-based `DataFrame` adapter. It covers the initial challenges, failed attempts, and the final successful strategies.

## Objective

To extend the reflection-based adapter to correctly serialize and deserialize arbitrarily nested data classes and polymorphic sealed hierarchies, making it fully compatible with idiomatic Kotlin.

## Current Status: **RESOLVED**

Both nested data classes and sealed hierarchies are now fully supported by the reflection-based adapter. The `wip_nested_types` package has been removed as the work is complete and integrated into the main `pragmatic` implementation.

## Initial Failing Test Case

The problem was initially isolated with the `PragmaticNestedTest`, which attempted a simple round-trip of a `SimpleUser` object containing a nested `SimpleProfile` object. This test now passes, along with new tests for sealed classes.

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

sealed class ApiResponse {
    data class Success(val data: String) : ApiResponse()
    data class Error(val errorCode: Int, val message: String) : ApiResponse()
}
```

## Path to Solution: Nested Data Classes

### The Problem (`[IMPL-SER-3]` - Initial Encounter)

The primary roadblock for nested data classes was the Kotlin reflection error encountered during serialization:
`Receiver type 'KProperty1<out Any, *>' contains out projection which prohibits the use of 'fun get(receiver: T): V'.`

This error occurred when attempting to recursively access property values from an `obj: Any` using `prop.get(obj)`. The compiler correctly identified this as a type-unsafe operation, as it could not guarantee that the `obj` of type `Any` was a valid receiver for a property of type `KProperty1<out Any, *>`.

### Failed Attempt 1: Simple Recursive `Row` Creation (Initial Flaw)

*   **Approach:** The `convertKotlinValueToSpark` function was modified to check `if (value::class.isData)`. If true, it would recursively call a helper to convert the nested object into a `GenericRowWithSchema`.
*   **Failure:** This immediately resulted in the `Receiver type ... contains out projection` compilation error, as the generic `Any` type was not specific enough for the reflection API.
*   **Root Cause:** This was a fundamental limitation of Kotlin's reflection API when used in an overly generic context.

### Failed Attempt 2: The Map-based Approach (Flawed Assumption)

*   **Approach:** The entire serialization logic was changed to convert the Kotlin object graph into a nested `List` of `Map<String, Any?>`, with the intent of using a (presumed) `spark.createDataFrame(List<Map>)` method.
*   **Failure:** This resulted in a `None of the following functions can be called with the arguments supplied` compilation error.
*   **Root Cause:** The presumed `createDataFrame(List<Map>)` API does not exist in the Spark Connect `SparkSession`. This approach was based on a flawed assumption about the available API surface.

### The Solution: The Type-Safe Generic Helper (`[IMPL-SER-3]` - Resolved)

The problem was finally solved by introducing a **type-safe generic helper function** (`private fun <T : Any> convertSpecificObjectToRow(obj: T, schema: StructType): Row`).

*   **Mechanism:** This helper function captures the specific type `T` of the object being processed. The `convertKotlinObjectToRow(obj: Any, schema: StructType)` function acts as a non-recursive dispatcher, calling this type-safe helper.
*   **Resolution:** By preserving the exact type `T`, the `KProperty1` objects obtained from `kClass.memberProperties` become `KProperty1<T, *>`. The call `prop.get(obj)` is then provably type-safe, as the receiver `obj` (of type `T`) perfectly matches the property's expected receiver type. This completely bypasses the `out projection` error.
*   **Recursion:** The `convertKotlinValueToSpark` function now calls this type-safe helper recursively when it encounters another data class, enabling deep nesting.

## Path to Solution: Sealed Classes (Tagged Union Strategy)

### The Problem

Spark's DataFrame schema is static and does not natively support polymorphic types like Kotlin's sealed classes, where a single type can have multiple distinct subtypes with different fields.

### The Solution: The Tagged Union Strategy

This problem was solved by implementing a "Tagged Union" strategy:

1.  **Schema Inference:**
    *   When `inferSchema` encounters a `KClass.isSealed`, it inspects all `sealedSubclasses`.
    *   It collects **all unique properties** from all possible subclasses.
    *   It constructs a single, unified `StructType` that includes all these properties (making them nullable, as not all properties will exist in all subtypes).
    *   **Crucially**, it adds a new, non-nullable `_type: String` field to the schema. This field acts as the "tag" for the union.

2.  **Serialization (Kotlin -> Spark):**
    *   When `convertKotlinValueToSpark` encounters a `value::class.isSealed`, it calls `convertKotlinObjectToRow`.
    *   `convertKotlinObjectToRow` now uses the unified schema. It writes the `kClass.simpleName` (e.g., "Success", "Error") of the concrete instance into the `_type` field.
    *   It populates only the fields relevant to the current concrete subtype, leaving other fields (from other subtypes) as `null`.

3.  **Deserialization (Spark -> Kotlin):**
    *   The `Dataset<Row>.toKotlinList` function first reads the `_type` field from the incoming `Row`.
    *   It then uses this `_type` string to dynamically find the correct `KClass` (subtype) from the sealed hierarchy's `sealedSubclasses`.
    *   Finally, it uses the primary constructor of that specific subtype to deserialize the `Row` into the correct Kotlin object.

## Conclusion

The successful implementation of these strategies means the reflection-based adapter now provides comprehensive support for complex Kotlin type hierarchies, making it a robust solution for idiomatic Kotlin data processing with Spark Connect.
