package spark.kotlin.reflect

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

class DataFrameWindowTest : SparkTestBase() {

    data class Sale(var department: String = "", var employee: String = "", var amount: Int = 0)
    data class RankedSale(var employee: String = "", var amount: Int = 0, var rank: Int = 0)
    data class DenseRankedSale(var employee: String = "", var amount: Int = 0, var rank: Int = 0, var dense_rank: Int = 0)

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data = listOf(
            Sale("Sales", "John", 1000),
            Sale("Sales", "Jane", 1200),
            Sale("Sales", "John", 800),
            Sale("Engineering", "Alice", 1500),
            Sale("Engineering", "Bob", 1400)
        )
        df = data.toDataFrame(spark)
    }

    @Test
    fun `should calculate row_number over a window`() {
        val windowSpec = Window.partitionBy("department").orderBy(col("amount").desc())
        
        val rankedDF = df.withColumn("rank", row_number().over(windowSpec))
        rankedDF.show()

        val results = rankedDF.toKotlinList<RankedSale>()
        
        val janeRow = results.find { it.employee == "Jane" }
        val johnRow = results.find { it.employee == "John" && it.amount == 1000 }

        assertEquals(1, janeRow!!.rank)
        assertEquals(2, johnRow!!.rank)
    }

    @Test
    fun `should calculate rank and dense_rank over a window`() {
        val windowSpec = Window.partitionBy("department").orderBy(col("amount").desc())

        val rankedDF = df.withColumn("rank", rank().over(windowSpec))
                           .withColumn("dense_rank", dense_rank().over(windowSpec))
        
        val results = rankedDF.toKotlinList<DenseRankedSale>()
        val aliceRow = results.find { it.employee == "Alice" }
        
        assertNotNull(aliceRow)
        assertEquals(1, aliceRow!!.rank)
        assertEquals(1, aliceRow.dense_rank)
    }

    @Test
    fun `should calculate aggregate functions over a window`() {
        val windowSpec = Window.partitionBy("department")

        val aggDF = df.withColumn("dept_total", sum("amount").over(windowSpec))
        aggDF.show()

        val salesTotal = aggDF.filter("department = 'Sales'").first().getLong(3)
        assertEquals(3000, salesTotal)
    }
}
