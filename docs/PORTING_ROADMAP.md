# Porting Roadmap: `kotlin-spark-api` to Spark Connect

This document outlines the plan for porting the features of the original, monolithic `kotlin-spark-api` to be compatible with the modern Spark Connect client-server architecture.

## Core Architectural Finding

Our research has proven that the original API's core mechanism, `encoder<T>()` (which relies on `Encoders.bean()`), is fundamentally incompatible with Spark Connect for idiomatic Kotlin data classes.

Therefore, a direct port is impossible. Instead, we must re-implement the *intent* of the original DSL using two new core strategies we have developed:
1.  **The Reflection-Based Adapter:** Our `toDataFrame()` and `toKotlinList()` functions, which correctly serialize/deserialize complex Kotlin objects.
2.  **The UDF-Wrapping Strategy:** Automatically wrapping user-provided lambdas in UDFs to allow for distributed execution on Spark Connect.

## File-by-File Analysis & Porting Plan

Here is the breakdown of the original API files and our plan for each.

### Tier 1: High Priority (Core DSL)

These files represent the most value to the end-user and are our top priority.

| File | Analysis & Plan | Status |
| :--- | :--- | :--- |
| **`Dataset.kt`** | **The most important file.** Contains the core DSL functions like `map`, `filter`, `flatMap`, `groupByKey`. <br> **Plan:** Port these functions one by one using the **UDF-Wrapping Strategy**. This is our next major research task. | **Partially Solved** <br> (`toKotlinList` is complete) |
| **`SparkSession.kt`** | Contains `toDS` and `toDF` helpers. <br> **Plan:** We have already implemented the superior, reflection-based `toDataFrame` which replaces this functionality. This is complete. | **Solved** |
| **`Column.kt`** | Contains helper functions and operator overloads for the `Column` class. <br> **Plan:** These are likely directly portable as they deal with creating query plans, not data serialization. Porting these will significantly improve the query DSL. | **Pending** |
| **`UserDefinedFunctions.kt`** | Contains helpers for creating UDFs. <br> **Plan:** We have a basic working UDF implementation. We should review this file to see if we can align our helpers with the original DSL for a better user experience (e.g., providing `udf` overloads for different numbers of arguments). | **Partially Solved** |

### Tier 2: Medium Priority (Advanced DSL)

Once the core DSL is complete, we can move on to these more advanced features.

| File | Analysis & Plan | Status |
| :--- | :--- | :--- |
| **`KeyValueGroupedDataset.kt`** | Extensions for typed grouping operations. <br> **Plan:** Porting this depends on having a robust, typed `Dataset.map()` implementation first. | **Blocked** |
| **`UserDefinedAggregateFunction.kt`** | Support for custom aggregate functions (UDAFs). <br> **Plan:** This is a separate, advanced topic. It will require its own research spike after the core DSL is complete. | **Pending** |

### Tier 3: Obsolete (Do Not Port)

These files are fundamentally incompatible with the Spark Connect architecture and should not be ported.

| File | Reason for Obsolescence | Status |
| :--- | :--- | :--- |
| **`Rdd.kt`** | Spark Connect does not have an RDD API. | **Obsolete** |
| **`RddDouble.kt`** | Spark Connect does not have an RDD API. | **Obsolete** |
| **`RddKeyValue.kt`** | Spark Connect does not have an RDD API. | **Obsolete** |
| **`Conversions.kt`** | Contains RDD-to-Dataset conversions. | **Obsolete** |
| **`Iterators.kt`** | Contains RDD-related helpers. | **Obsolete** |
| **`Encoding.kt`** | The `encoder<T>()` mechanism is the root of the incompatibility. Our reflection adapter replaces it entirely. | **Obsolete** |
| **`Arities.kt`** | Deprecated tuple-like classes that are no longer needed. | **Obsolete** |

### Tier 4: Future Work (Streaming)

These files are for Structured Streaming, which is a separate effort from the core DataFrame API.

| File | Analysis & Plan | Status |
| :--- | :--- | :--- |
| **`DataStreamWriter.kt`** | Streaming-specific. | **Future Work** |
| **`GroupState.kt`** | Streaming-specific. | **Future Work** |
| **`StreamingKeyValues.kt`** | Streaming-specific. | **Future Work** |

## Recommended Next Steps

Based on this analysis, the clear path forward is:

1.  **Port `Dataset.kt`:** Begin with the `map` function. Use our test-driven approach: create a failing test, implement the UDF-wrapping strategy, and make it pass.
2.  **Port `Column.kt`:** After `map`, porting the `Column` extensions will provide the biggest boost to the usability of the query DSL.
