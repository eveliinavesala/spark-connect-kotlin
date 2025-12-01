# Test Suite Overview

This document provides a comprehensive overview of the test suite for the Kotlin Spark Connect compatibility layer. It outlines the purpose of each test category, highlights key findings, and tracks the current status of our development.

## 1. Core Functionality Tests (`pragmatic` package)

**Goal:** To validate the fundamental serialization and deserialization capabilities of our reflection-based adapter for various Kotlin data types and structures. These tests ensure that our `List<T>.toDataFrame(spark)` and `Dataset<Row>.toKotlinList<T>()` functions work as expected.

**Key Findings:**
*   **Primitives & Basic Data Classes:** Successfully converts lists of simple data classes (e.g., `Person(name: String, age: Int)`).
*   **Collections:** Successfully handles `List<String>`, `Set<String>`, and `Map<String, Int>` within data classes.
*   **Enums:** Successfully serializes and deserializes Kotlin `enum` classes.
*   **Nested Data Classes:** Successfully handles arbitrarily nested data classes, proving the robustness of our recursive reflection logic.
*   **Sealed Classes/Interfaces:** Successfully implements the "tagged union" strategy for `sealed` hierarchies, allowing for serialization and deserialization of polymorphic data.

**Test Files:**
*   `pragmatic/PragmaticTest.kt`
*   `pragmatic/PragmaticCollectionsTest.kt`
*   `pragmatic/PragmaticEnumTest.kt`
*   `pragmatic/PragmaticAdvancedTypesTest.kt` (combines nested and sealed class tests)

**Current Status:** **All tests in this category are passing.** This confirms the core functionality of our adapter.

## 2. Data Integrity & Edge Case Tests (`data_integrity` package)

**Goal:** To validate the adapter's behavior with null values, type mismatches, and to demonstrate the limitations of the native Spark API.

**Key Findings:**
*   **Null Safety:** Our adapter correctly handles nullable properties and throws `IllegalArgumentException` when a `null` value is encountered for a non-nullable Kotlin property during deserialization.
*   **Native API Limitations:** Explicitly demonstrates that the native `spark.createDataset(..., Encoders.bean(...))` fails for idiomatic Kotlin data classes due to the no-argument constructor requirement.

**Test Files:**
*   `data_integrity/DataIntegrityTest.kt`

**Current Status:** **All tests in this category are passing.** This confirms the robustness of our adapter and highlights the necessity of our solution.

## 3. Real-World ETL Scenario Tests (`spark_etl` package)

**Goal:** To validate the end-to-end functionality of our adapter and UDF capabilities in a more complex, real-world data processing scenario.

**Key Findings:**
*   **CSV Ingestion & Schema Inference:** Successfully reads a CSV file, infers schema, and performs basic DataFrame operations.
*   **UDF Integration:** Demonstrates the successful registration and execution of Kotlin UDFs in a Spark Connect environment.
*   **Data Transformation:** Validates common ETL transformations like column selection, renaming, filtering, and creating new columns.

**Test Files:**
*   `spark_etl/SparkETLTest.kt`
*   `pragmatic/PragmaticUDFTest.kt`

**Current Status:** **All tests in this category are passing.** This confirms the practical applicability of our solution.

## 4. Native Spark API Limitation Tests (`classes` and `collections` packages)

**Goal:** To explicitly demonstrate the incompatibility of the native Spark Connect API with various idiomatic Kotlin constructs when relying on `Encoders.bean()`. These tests serve as the problem statement that our `pragmatic` adapter solves.

**Key Findings:**
*   **`NoSuchMethodException`:** Many tests fail because `Encoders.bean()` cannot find a no-argument constructor for Kotlin data classes, enums, or value classes.
*   **`IllegalAccessException`:** Failures related to Spark's inability to access private constructors or members.
*   **`IllegalStateException: found an unhandled type: null`:** Indicates issues with Spark's type inference for complex Kotlin types like sealed interfaces.
*   **`Unsupported collection type: interface java.util.Set`:** Demonstrates Spark's inability to natively handle Kotlin `Set` types.

**Test Files:**
*   `classes/*.kt` (e.g., `IdiomaticDataClassTest.kt`, `IdiomaticEnumTest.kt`, `IdiomaticSealedClassTest.kt`)
*   `collections/*.kt` (e.g., `IdiomaticListTest.kt`, `IdiomaticSetTest.kt`, `IdiomaticMapTest.kt`)

**Current Status:** **These tests are expected to fail.** Their failures highlight the limitations of the native Spark API and underscore the value of our `pragmatic` adapter.

## Obsolete Tests

*   **`classes/JvmWrapped*` tests:** These tests were part of an earlier investigation into JVM bytecode generation and are no longer relevant to our Spark Connect compatibility goals. They have been removed.
*   **`wip_nested_types` tests:** These were temporary tests for nested and sealed classes. Their content has been moved to `pragmatic/PragmaticAdvancedTypesTest.kt` and the temporary files/directory have been removed.
