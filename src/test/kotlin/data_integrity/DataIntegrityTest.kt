package data_integrity

import classes.SparkTestBase
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

class DataIntegrityTest : SparkTestBase() {

    // --- 1. Type Safety Tests ---

    @Test
    fun `dataframe api is not type-safe at compile time`() {
        val data = listOf(TypedPerson("Alice", 30))
        val df = data.toDataFrame(spark)

        val query = df.select(functions.col("name"), functions.col("ag"))

        try {
            query.collect()
            fail("Expected a runtime AnalysisException, but none was thrown.")
        } catch (e: AnalysisException) {
            // Success!
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
    fun `reflection engine correctly handles null values`() {
        val dataWithNulls = listOf(
            ItemWithNullable(1, "A valid description"),
            ItemWithNullable(2, null)
        )

        val df = dataWithNulls.toDataFrame(spark)
        val results = df.toKotlinList<ItemWithNullable>()

        assertEquals(2, results.size)
        assertEquals("A valid description", results.find { it.id == 1 }?.description)
        assertNull(results.find { it.id == 2 }?.description)
    }

    @Test
    fun `toKotlinList throws when non-nullable field receives null`() {
        val dataWithNulls = listOf(
            ItemWithNullable(1, "A valid description"),
            ItemWithNullable(2, null)
        )
        val df = dataWithNulls.toDataFrame(spark)

        assertThrows(IllegalArgumentException::class.java) {
            df.toKotlinList<ItemWithNonNullable>()
        }
    }
}
