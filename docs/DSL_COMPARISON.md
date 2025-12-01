# DSL Comparison: `kotlin-spark-api` vs. Spark Ecosystem

This document compares the Data-Specific Language (DSL) of the original `kotlin-spark-api`, our current `pragmatic` solution, and the native Scala and PySpark DSLs. The goal is to understand how our API aligns with Spark's fundamental design principles and to inform future API decisions.

## Core Spark DSL Philosophy

Fundamentally, Spark's primary data abstraction is the **DataFrame** (represented as `Dataset<Row>` in Scala/Java/Kotlin). While Scala introduced `Dataset<T>` for compile-time type safety, the vast majority of Spark's core operations and its Python/R APIs operate on the untyped `DataFrame` abstraction.

The original `kotlin-spark-api` attempted to heavily leverage `Dataset<T>` for a fully typed experience, often relying on `Encoders.bean()`. Our research has shown this approach is fundamentally incompatible with idiomatic Kotlin data classes in a Spark Connect environment.

## Comparison of Core Operations

We will compare two fundamental operations:
1.  **Creating a DataFrame from a local collection.**
2.  **Collecting a DataFrame back into a local collection.**

### 1. Creating a DataFrame from a Local Collection

| Language/API | Example DSL | Analysis |
| :--- | :--- | :--- |
| **Original `kotlin-spark-api`** | `val df = spark.toDF(listOf(Person("Alice", 30)))` <br> `val ds = spark.toDS(listOf(Person("Alice", 30)))` | - Aims for `Dataset<T>` (via `toDS`) or `Dataset<Row>` (via `toDF`). <br> - Relies on `encoder<T>()` which uses `Encoders.bean()`. <br> - **Fails for idiomatic Kotlin data classes in Spark Connect.** |
| **Our `pragmatic` DSL** | `val df = listOf(Person("Alice", 30)).toDataFrame(spark)` | - Explicitly creates a `Dataset<Row>` (DataFrame). <br> - Uses our robust reflection adapter. <br> - **Works reliably for idiomatic Kotlin data classes in Spark Connect.** <br> - Is an extension on `List`, making it feel natural. |
| **Scala Spark** | `val df = spark.createDataFrame(Seq(Person("Alice", 30)))` <br> `val ds = spark.createDataset(Seq(Person("Alice", 30)))` | - `createDataFrame` is the primary method. <br> - `createDataset` is available but requires an `Encoder[T]`. <br> - `Seq` is Scala's collection type. |
| **PySpark** | `df = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])` | - `createDataFrame` is the primary method. <br> - Works with Python lists of tuples or dicts. <br> - Untyped by nature. |

### 2. Collecting a DataFrame back into a Local Collection

| Language/API | Example DSL | Analysis |
| :--- | :--- | :--- |
| **Original `kotlin-spark-api`** | `val list = df.toList<Person>()` | - Aims for `List<T>`. <br> - Relies on `encoder<T>()` for deserialization. <br> - **Fails for idiomatic Kotlin data classes in Spark Connect.** |
| **Our `pragmatic` DSL** | `val list = df.toKotlinList<Person>()` | - Explicitly collects to `List<T>`. <br> - Uses our robust reflection adapter. <br> - **Works reliably for idiomatic Kotlin data classes in Spark Connect.** <br> - Is an extension on `Dataset<Row>`. |
| **Scala Spark** | `val list = df.as[Person].collect().toList` <br> `val list = ds.collect().toList` | - `collect()` returns an `Array[T]` or `Array[Row]`. <br> - Requires `as[T]` for typed conversion before collection. |
| **PySpark** | `list = df.collect()` | - `collect()` returns a list of `Row` objects. <br> - Requires manual conversion from `Row` to Python objects. |

## Analysis and Recommendation

1.  **Alignment with Spark's Core DSL:**
    *   Our `pragmatic` DSL (`List<T>.toDataFrame`, `Dataset<Row>.toKotlinList`) aligns more closely with the **fundamental DataFrame-centric nature of Spark's Scala and Python APIs**. These APIs primarily operate on DataFrames (`Dataset<Row>`) and provide mechanisms to convert to/from local collections.
    *   The original `kotlin-spark-api`'s heavy reliance on `Dataset<T>` via `Encoders.bean()` was an attempt to provide a fully typed experience, but this has proven to be a significant impedance mismatch with Spark Connect and idiomatic Kotlin.

2.  **Breaking Changes vs. New API:**
    *   Our `toDataFrame` is a new function. It does not directly conflict with the original `toDF` signatures, but provides the same core functionality.
    *   Our `toKotlinList` is a new function. It does not directly conflict with the original `toList` signatures, but provides the same core functionality.
    *   The original `toDS` functions are fundamentally incompatible with our approach as they promise a `Dataset<T>` return type which our adapter cannot provide without an `Encoder<T>`.

## Recommendation for Final API Design

Given the fundamental incompatibility of the original `encoder<T>()` mechanism with Spark Connect and idiomatic Kotlin, and the strong alignment of our `pragmatic` solution with Spark's DataFrame-centric ecosystem:

**We should adopt our current `List<T>.toDataFrame(spark)` and `Dataset<Row>.toKotlinList<T>()` as the primary, recommended DSL for data conversion in Spark Connect.**

This approach:
*   **Is robust and proven to work** for all complex Kotlin types (nested, sealed, collections, enums).
*   **Aligns with Spark's core DataFrame abstraction** as seen in Scala and PySpark.
*   **Provides a clear, idiomatic Kotlin DSL** that hides the Spark Connect complexities.
*   **Avoids trying to force an incompatible `Dataset<T>` model** that relies on `Encoders.bean()`.

We can then provide clear deprecation messages for the original `toDS` and `toList` functions, guiding users to our new, compatible API.

---

## Bringing Our DSL Closer to Scala/PySpark Variants

To make our DSL even more familiar to developers coming from Scala Spark or PySpark, we can consider the following refinements, while still leveraging our robust reflection-based implementation.

### 1. Creating a DataFrame from a Local Collection

*   **Scala/PySpark Pattern:** The primary method is `spark.createDataFrame(...)`.
*   **Our Current DSL:** `listOf(...).toDataFrame(spark)`
*   **Proposed Refinement:** Introduce a `SparkSession.createDataFrame` extension function that mirrors the Scala/PySpark pattern.

    ```kotlin
    // Proposed new DSL
    val df = spark.createDataFrame(listOf(Person("Alice", 30)))
    ```

    **Implementation Idea:**
    ```kotlin
    inline fun <reified T : Any> SparkSession.createDataFrame(data: List<T>): Dataset<Row> {
        return data.toDataFrame(this) // Internally calls our existing List<T>.toDataFrame(spark)
    }
    ```
    **Benefit:** This provides a direct, familiar entry point for DataFrame creation that aligns perfectly with the native Spark APIs.

### 2. Collecting a DataFrame back into a Local Collection

*   **Scala Spark Pattern:** `df.as[Person].collect().toList` (requires `Encoder[Person]`) or `df.collect().map(row => ...)`
*   **PySpark Pattern:** `df.collect()` (returns `List[Row]`)
*   **Our Current DSL:** `df.toKotlinList<Person>()`
*   **Proposed Refinement:** While `toKotlinList` is clear, we could also provide a `collectAsList<T>()` extension on `Dataset<Row>` to mirror the `collect()` pattern more closely, especially if we want to provide a direct `List<Row>` collection.

    ```kotlin
    // Proposed new DSL
    val people: List<Person> = df.collectAsList<Person>()
    val rows: List<Row> = df.collectAsList() // If we want to allow collecting as List<Row>
    ```

    **Implementation Idea:**
    ```kotlin
    inline fun <reified T : Any> Dataset<Row>.collectAsList(): List<T> {
        return this.toKotlinList() // Internally calls our existing Dataset<Row>.toKotlinList()
    }
    ```
    **Benefit:** This provides a more direct parallel to the `collect()` method in other Spark APIs, while still leveraging our robust deserialization.

### 3. Handling `Dataset<T>` (Typed Datasets)

*   **Scala Spark Pattern:** `ds.as[Person]` or `ds.map[NewPerson](...)`
*   **Our Current DSL:** We explicitly work with `Dataset<Row>`.
*   **Proposed Refinement:** This is the most challenging area. The original `kotlin-spark-api`'s `Dataset<T>.map` and `Dataset<*>.as<R>()` functions are highly valued for type safety.
    *   **`Dataset<*>.as<R>()`:** We could implement this to convert a `Dataset<Row>` to a "logically typed" `Dataset<R>` (which is still `Dataset<Row>` under the hood) that uses our reflection logic for subsequent operations.
    *   **`Dataset<T>.map(...)`:** This would require a UDF-based approach, where the lambda `(T) -> R` is automatically wrapped in a UDF, executed on the server, and the result is then converted back to `Dataset<Row>`. This is a significant piece of work.

    **Benefit:** This would bring back the highly desired compile-time type safety for transformations, making the Kotlin DSL feel even more native.

## Conclusion on Refinements

By introducing these new functions (`spark.createDataFrame`, `df.collectAsList`), we can provide a DSL that is both idiomatic Kotlin and highly familiar to developers experienced with Scala Spark and PySpark, all powered by our robust reflection-based solution. The implementation of typed transformations like `map` would be the next major step in achieving full DSL parity.
