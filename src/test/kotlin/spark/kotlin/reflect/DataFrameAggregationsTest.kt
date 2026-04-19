package spark.kotlin.reflect

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DataFrameAggregationsTest : SparkTestBase() {
    data class Sale(
        var course: String = "",
        var city: String = "",
        var sales: Int = 0,
    )

    data class AggResult(
        var city: String = "",
        var avgSales: Double = 0.0,
        var countStar: Long = 0,
    )

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data =
            listOf(
                Sale("Java", "Helsinki", 100),
                Sale("Kotlin", "Helsinki", 150),
                Sale("Java", "Turku", 200),
                Sale("Kotlin", "Turku", 250),
            )
        df = data.toDataFrame(spark)
    }

    // --- 3. Aggregations ---

    @Test
    fun `groupBy() and agg() should perform aggregations`() {
        val grouped = df.groupBy("city").agg(avg("sales").alias("avgSales"), count("*").alias("countStar"))
        val results = grouped.toKotlinList<AggResult>()

        assertEquals(2, results.size)
        val helsinkiRow = results.find { it.city == "Helsinki" }
        assertNotNull(helsinkiRow)
        assertEquals(125.0, helsinkiRow!!.avgSales, 0.001)
        assertEquals(2L, helsinkiRow.countStar)
    }

    @Test
    fun `cube() should create a multi-dimensional cube`() {
        val cubed = df.cube("city", "course").agg(sum("sales"))
        assertEquals(9, cubed.count())
    }

    @Test
    fun `rollup() should create a multi-dimensional rollup`() {
        val rolledUp = df.rollup("city", "course").agg(sum("sales"))
        assertEquals(7, rolledUp.count())
    }

    @Test
    fun `summary() and describe() should compute statistics`() {
        val summary = df.summary("count", "mean")
        assertEquals(4, summary.columns().size)
        assertEquals(2, summary.count())

        val describe = df.describe("sales")
        assertEquals(2, describe.columns().size)
        assertEquals(5, describe.count())
    }
}
