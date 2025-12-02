# Investigation Log: Nested & Sealed Types (RESOLVED)

This document originally tracked the challenges in supporting nested data classes and sealed hierarchies. This investigation has concluded successfully, and robust solutions have been implemented.

## Objective

To extend the reflection-based adapter to correctly serialize and deserialize arbitrarily nested data classes and polymorphic sealed hierarchies.

## Current Status: **RESOLVED**

Both nested data classes and sealed hierarchies are now fully supported by the reflection-based adapter. The `wip_nested_types` package has been removed as the work is complete.

## Solution for Nested Data Classes

### The Problem (`[IMPL-SER-3]` Revisited)

The primary roadblock for nested data classes was the Kotlin reflection error:
`Receiver type 'KProperty1<out Any, *>' contains out projection which prohibits the use of 'fun get(receiver: T): V'.`
This occurred in our recursive serialization logic when trying to access property values from an `obj: Any`, as the compiler could not guarantee type safety.

### The Solution: The Type-Safe Generic Helper

The problem was solved by introducing a **type-safe generic helper function** (`private fun <T : Any> convertSpecificObjectToRow(obj: T, schema: StructType): Row`).

*   **Mechanism:** This helper function captures the specific type `T` of the object being processed.
*   **Resolution:** By preserving the exact type `T`, the `KProperty1` objects obtained from `kClass.memberProperties` become `KProperty1<T, *>`. The call `prop.get(obj)` is then provably type-safe, as the receiver `obj` (of type `T`) perfectly matches the property's expected receiver type.
*   **Recursion:** The `convertKotlinValueToSpark` function now calls this type-safe helper recursively when it encounters another data class, enabling deep nesting.

## Solution for Sealed Classes (Tagged Union Strategy)

### The Problem

Spark's DataFrame schema is static and does not natively support polymorphic types like Kotlin's sealed classes, where a single type can have multiple distinct subtypes with different fields.

### The Solution: The Tagged Union Strategy

This problem was solved by implementing a "Tagged Union" strategy:

1.  **Schema Inference:**
    *   When `inferSchema` encounters a `KClass.isSealed`, it inspects all `sealedSubclasses`.
    *   It collects **all unique properties** from all possible subclasses.
    *   It constructs a single, unified `StructType` that includes all these properties (making them nullable, as not all properties will exist in all subtypes).
    *   **Crucially**, it adds a new, non-nullable `_type: String` field to the schema.

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
