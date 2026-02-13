package pragmatic

import classes.SparkTestBase
import dataframe.selectTyped
import dataframe.toDataFrame
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RefinementTest : SparkTestBase() {

    data class Person(val name: String, val age: Int)

    @Test
    fun `toDataFrame on empty list should produce a DataFrame with a schema`() {
        val emptyList = emptyList<Person>()
        val df = emptyList.toDataFrame(spark)

        assertEquals(0, df.count())
        val columns = df.columns().toList()
        assertEquals(2, columns.size)
        assertTrue(columns.contains("name"))
        assertTrue(columns.contains("age"))
    }

    @Test
    fun `selectTyped should select columns safely`() {
        val data = listOf(Person("Alice", 30))
        val df = data.toDataFrame(spark)

        val selected = df.selectTyped(Person::age)
        
        assertEquals(1, selected.columns().size)
        assertEquals("age", selected.columns()[0])
        assertEquals(30, selected.first().getAs<Int>("age"))
    }
}
