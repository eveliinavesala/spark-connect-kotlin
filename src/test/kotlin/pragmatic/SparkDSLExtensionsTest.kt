package pragmatic

import classes.SparkTestBase
import dataframe.*
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

data class Person(var name: String = "", var age: Int = 0, var salary: Double = 0.0)

class SparkDSLExtensionsTest : SparkTestBase() {

    @Test
    fun `should use column operator extensions`() {
        val data = listOf(Person("Alice", 30, 1000.0))
        val df = data.toDataFrame(spark)

        val result = df.select(
            (col("age") + 10) * 2,
            col("salary") / 100
        ).first()

        assertEquals(80, result.getInt(0))
        assertEquals(10.0, result.getDouble(1), 0.001)
    }

    @Test
    fun `should use infix operators for filtering`() {
        val data = listOf(Person("Bob", 40, 2500.0))
        val df = data.toDataFrame(spark)

        val result = df.filter(col("age") gt 30 and (col("name") eq "Bob"))
        assertEquals(1, result.count())
    }

    @Test
    fun `should use typed getNullable extension for Row`() {
        val data = listOf(Person("Charlie", 50, 5000.0))
        val df = data.toDataFrame(spark)

        val row = df.first()

        val name: String? = row.getNullable("name")
        val age: Int? = row.getNullable("age")

        assertEquals("Charlie", name)
        assertEquals(50, age)
    }
    
    @Test
    fun `showPretty should display untruncated output`() {
        val data = listOf(
            Person("This is a very long name to demonstrate truncation", 99, 9999.99)
        )
        val df = data.toDataFrame(spark)
        
        println("--- Standard show() ---")
        df.show()
        
        println("\n--- showPretty() ---")
        df.showPretty()
    }
}
