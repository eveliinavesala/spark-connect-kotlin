package data_integrity

import classes.SparkTestBase
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import pragmatic.createPragmaticDataFrame
import pragmatic.toKotlinList

class DataIntegrityTest : SparkTestBase() {

    // --- 1. Type Safety Tests ---

    @Test
    fun `dataframe api is not type-safe at compile time`() {
        val data = listOf(TypedPerson("Alice", 30))
        val df = spark.createPragmaticDataFrame(data, TypedPerson::class)

        val query = df.select(functions.col("name"), functions.col("ag"))

        // The goal is just to prove a runtime exception happens.
        try {
            query.collect()
            fail("Expected a runtime AnalysisException, but none was thrown.")
        } catch (e: AnalysisException) {
            // Success! The test's purpose is fulfilled by catching the runtime exception.
        }
    }

    @Test
    fun `dataset api fails with bean encoder due to no-arg constructor`() {
        val data = listOf(TypedPerson("Alice", 30))
        
        assertThrows(Exception::class.java) {
            val ds = spark.createDataset(data, Encoders.bean(TypedPerson::class.java))
            ds.collect()
        }
    }

    // --- 2. Null Safety Tests ---

    @Test
    fun `pragmatic approach correctly handles null values`() {
        val dataWithNulls = listOf(
            ItemWithNullable(1, "A valid description"),
            ItemWithNullable(2, null)
        )

        val df = spark.createPragmaticDataFrame(dataWithNulls, ItemWithNullable::class)
        val results = df.toKotlinList(ItemWithNullable::class)

        assertEquals(2, results.size)
        assertEquals("A valid description", results.find { it.id == 1 }?.description)
        assertNull(results.find { it.id == 2 }?.description)
    }

    @Test
    fun `pragmatic approach fails to create non-nullable class with null data`() {
        val dataWithNulls = listOf(
            ItemWithNullable(1, "A valid description"),
            ItemWithNullable(2, null)
        )
        val df = spark.createPragmaticDataFrame(dataWithNulls, ItemWithNullable::class)

        // Expect our new, more informative IllegalArgumentException
        assertThrows(IllegalArgumentException::class.java) {
            df.toKotlinList(ItemWithNonNullable::class)
        }
    }
}
