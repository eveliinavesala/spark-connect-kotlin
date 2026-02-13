package dataframe

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DataFrameTransformationsTest : SparkTestBase() {

    data class Person(var name: String = "", var age: Int = 0)
    data class AgeOnly(var age: Int = 0)

    private lateinit var df: Dataset<Row>
    private lateinit var data: List<Person>

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

    // --- 2. Basic Transformations ---

    @Test
    fun `filter() and where() should filter rows`() {
        val filtered = df.filter(col("age").gt(30))
        val kotlinList = filtered.toKotlinList<Person>()
        assertEquals(2, kotlinList.size)
        assertTrue(kotlinList.all { it.age > 30 })
    }

    @Test
    fun `select() and selectExpr() should select columns`() {
        val selected = df.select("name")
        assertEquals(1, selected.columns().size)
        assertEquals("name", selected.columns()[0])
        
        val selectedExpr = df.selectExpr("age + 1 as age")
        val result = selectedExpr.toKotlinList<AgeOnly>().first()
        assertEquals(31, result.age)
    }

    @Test
    fun `drop() should remove a column`() {
        val dropped = df.drop("age")
        assertFalse(dropped.columns().contains("age"))
        val firstRow = dropped.first()
        assertEquals("Alice", firstRow.getAs<String>("name"))
    }

    @Test
    fun `withColumn() and withColumnRenamed() should modify columns`() {
        val withNewCol = df.withColumn("age_plus_5", col("age") + 5)
        assertEquals(35, withNewCol.first().getAs<Int>("age_plus_5"))

        val renamed = df.withColumnRenamed("age", "years")
        assertTrue(renamed.columns().contains("years"))
        assertEquals(30, renamed.first().getAs<Int>("years"))
    }

    @Test
    fun `distinct() and dropDuplicates() should remove duplicate rows`() {
        val duplicateData = data + listOf(Person("Alice", 30))
        val dfWithDupes = duplicateData.toDataFrame(spark)
        
        assertEquals(5, dfWithDupes.count())
        assertEquals(4, dfWithDupes.distinct().count())
        assertEquals(4, dfWithDupes.dropDuplicates().count())
    }

    @Test
    fun `limit() and offset() should restrict rows`() {
        assertEquals(2, df.limit(2).count())
        val offsetList = df.orderBy("name").offset(2).toKotlinList<Person>()
        assertEquals(2, offsetList.size)
        assertEquals("Charlie", offsetList.first().name)
    }

    @Test
    fun `orderBy() and sort() should sort the dataset`() {
        val sorted = df.orderBy(col("age"))
        val sortedList = sorted.toKotlinList<Person>()
        assertEquals("Charlie", sortedList.first().name)

        val sortedDesc = df.sort(col("name").desc())
        val sortedDescList = sortedDesc.toKotlinList<Person>()
        assertEquals("David", sortedDescList.first().name)
    }

    @Test
    fun `sample() should return a subset of the data`() {
        val count = df.sample(false, 0.5, 123L).count()
        assertEquals(3, count)
    }
}
