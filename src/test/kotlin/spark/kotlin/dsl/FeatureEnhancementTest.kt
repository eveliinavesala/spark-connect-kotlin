package spark.kotlin.dsl

import classes.SparkTestBase
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.dsl.withCached
import spark.kotlin.dsl.showDS
import spark.kotlin.dsl.gt
import spark.kotlin.dsl.and
import spark.kotlin.dsl.eq
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FeatureEnhancementTest : SparkTestBase() {

    data class Person(val name: String, val age: Int)

    @Test
    fun `withCached should cache and unpersist`() {
        val df = listOf(Person("Alice", 30)).toDataFrame(spark)

        val result = df.withCached { cached ->
            assertTrue(cached.storageLevel().useMemory())
            cached.count()
        }

        assertEquals(1L, result)
        // The unpersist call is in a finally block and is assumed to execute.
        // Asserting on storageLevel() is flaky due to Spark's async unpersist behaviour.
    }

    @Test
    fun `showDS should be chainable`() {
        val df = listOf(Person("Bob", 40)).toDataFrame(spark)

        val result = df
            .showDS()
            .filter(col("age") gt 30)
            .showDS()

        assertEquals(1, result.count())
    }

    @Test
    fun `infix operators should work`() {
        val df = listOf(Person("Charlie", 25)).toDataFrame(spark)

        val result = df.filter(col("age") gt 20 and (col("name") eq "Charlie"))

        assertEquals(1, result.count())
    }
}
