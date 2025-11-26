# Future Testing Plan for Spark-Kotlin Interaction

This document outlines a plan for creating a comprehensive test suite that covers advanced scenarios and potential pitfalls when using Kotlin with Apache Spark. The goal is to ensure the robustness, correctness, and performance of Spark applications written in Kotlin.

## 1. Serialization and Encoders

The interaction between Kotlin's class features and Spark's serialization mechanism is critical. The default `Encoders.bean()` is not always sufficient for idiomatic Kotlin code.

### Test Plan:
- **Custom `Encoder` for Complex Types:** Create a test case for a data class that `Encoders.bean()` cannot handle (e.g., a class containing a `List` of a custom type). Implement and test a custom Spark `Encoder` for this class.
- **Kryo Serialization Fallback:**
  - Create a test with a complex, nested data class that is known to fail with the default encoder.
  - Configure Spark to use the Kryo serializer.
  - Verify that the `Dataset` can be created and manipulated successfully when Kryo is enabled. This will demonstrate the fallback mechanism for types that are too complex for the bean encoder.
- **Test `Serializable` Interface:** For classes that are not simple data holders (e.g., classes with complex logic), test the impact of implementing `java.io.Serializable` and how it interacts with Spark's `Dataset` API.

## 2. Null Safety

This is a primary source of runtime errors when integrating strongly-typed Kotlin with Spark's schema-based, nullable world.

### Test Plan:
- **Nulls in `DataFrame` to `Dataset` Conversion:**
  - Create a JSON or CSV file with `null` values for fields that correspond to non-nullable properties in a Kotlin data class (e.g., `val name: String`).
  - In a test, read this data into a `DataFrame`.
  - Attempt to convert the `DataFrame` to a `Dataset` of the Kotlin data class (`.as<MyClass>()`).
  - **Expected Outcome:** The test should fail with a `RuntimeException` wrapping a `NullPointerException`. This test explicitly documents a common and critical failure mode.
- **Handling Nulls Gracefully:**
  - Create a second version of the data class where the corresponding properties are nullable (e.g., `val name: String?`).
  - Run the same test as above and verify that the conversion to a `Dataset` now succeeds.
  - Add assertions to check that the values are indeed `null` in the resulting `Dataset`. This demonstrates the correct way to model data that may contain nulls.

## 3. Performance Implications (Dataset vs. DataFrame)

While not a functional test, it's important to understand and document the performance trade-offs between the type-safe `Dataset` API and the optimized `DataFrame` API.

### Plan:
- **Benchmark `Dataset` vs. `DataFrame` Operations:**
  - Create a large dataset (e.g., 1 million+ rows).
  - Write a benchmark test (potentially using a library like JMH, or a simple timing mechanism for a basic test) that performs the same business logic using two different implementations:
    1. **`DataFrame` API:** Using `withColumn`, `select`, and `filter` with string-based column expressions.
    2. **`Dataset` API:** Using `.map()`, `.filter()`, and other typed transformations.
  - The test should measure and log the execution time for both versions. This will provide concrete data on the performance overhead of deserializing objects for `Dataset` operations.

## 4. User-Defined Functions (UDFs)

UDFs are a common extension point and another area where serialization can cause issues.

### Test Plan:
- **UDF with a Simple Data Class:**
  - Create a UDF that accepts a simple Kotlin data class as an argument.
  - Register and use this UDF in a `DataFrame` operation.
  - Verify that the UDF executes correctly, confirming that the object can be serialized and passed to the UDF.
- **UDF with a Complex Object:**
  - Create a UDF that accepts a more complex Kotlin object (e.g., one containing nested objects or collections).
  - Test this UDF to identify potential serialization failures when the object is sent to Spark executors. This helps validate which object structures are safe to use within UDFs.
- **UDF Returning a Custom Type:**
  - Create a UDF that returns a Kotlin data class.
  - Test this UDF and verify that Spark can correctly handle the returned object, which involves deserialization on the driver.
