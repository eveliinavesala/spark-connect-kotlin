package dataframe

import classes.SparkTestBase
import org.apache.spark.api.java.function.ForeachFunction
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.api.java.function.ReduceFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reflection.toDataFrame
import reflection.toKotlinList

class DataFrameActionsTest : SparkTestBase() {

    data class Person(var name: String = "", var age: Int = 0)

    private lateinit var df: Dataset<Row>
    private lateinit var data: List<Person>

    companion object {
        // Define UDFs in a static context to ensure they are serializable
        val oldestRowFunc = ReduceFunction<Row> { r1, r2 -> if (r1.getInt(r1.fieldIndex("age")) > r2.getInt(r2.fieldIndex("age"))) r1 else r2 }
        val namePrinter = ForeachFunction<Row> { row -> println(row.getAs<String>("name")) }
        val partitionPrinter = ForeachPartitionFunction<Row> { iterator -> iterator.forEach { row -> println(row.getAs<String>("name")) } }
    }

    @BeforeEach
    fun setup() {
        data = listOf(
            Person("Alice", 30),
            Person("Bob", 40),
            Person("Charlie", 25),
            Person("David", 35)
        )
        df = data.toDataFrame(spark)
    }

    // --- 1. Core Actions ---

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `collect() and collectAsList() should return all rows`() {
        val collectedAsArray = df.collect() as Array<Row>
        val collectedAsList = df.collectAsList()

        assertEquals(4, collectedAsArray.size)
        assertEquals(4, collectedAsList.size)
        
        val kotlinList = df.toKotlinList<Person>()
        assertTrue(kotlinList.containsAll(data))
    }

    @Test
    fun `count() should return the correct number of rows`() {
        assertEquals(4L, df.count())
    }

    @Test
    fun `first() and head() should return the first row`() {
        val firstRow = df.orderBy("name").first() // Order for deterministic result
        assertEquals("Alice", firstRow.getAs<String>("name"))
        assertEquals(30, firstRow.getAs<Int>("age"))
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `take() and takeAsList() should return the first n rows`() {
        val orderedDf = df.orderBy("name")
        val takenArray = orderedDf.take(2) as Array<Row>
        val takenList = orderedDf.takeAsList(2)

        assertEquals(2, takenArray.size)
        assertEquals("Bob", takenList[1].getAs<String>("name"))
    }
    
    @Test
    @Suppress("UNCHECKED_CAST")
    fun `tail() should return the last n rows`() {
        val orderedDf = df.orderBy("name")
        val tailRows = orderedDf.tail(2) as Array<Row>
        assertEquals(2, tailRows.size)
        assertEquals("David", tailRows[1].getAs<String>("name"))
    }

    @Test
    fun `isEmpty() should return true for empty dataframe and false for non-empty`() {
        assertTrue(spark.emptyDataFrame().isEmpty)
        assertFalse(df.isEmpty)
    }
    
    @Test
    fun `show() should display the dataframe without error`() {
        df.show()
    }

    @Test
    fun `foreach() should apply a function to each row`() {
        assertDoesNotThrow { df.foreach(namePrinter) }
    }

    @Test
    fun `foreachPartition() should apply a function to each partition`() {
        assertDoesNotThrow { df.repartition(2).foreachPartition(partitionPrinter) }
    }

    @Test
    fun `reduce() should be replaced with aggregation for DataFrames`() {
        val oldestRow = df.selectExpr("max(age) as max_age").first()
        assertEquals(40, oldestRow.getInt(0))
    }
}
