package pragmatic

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import pragmatic.toDataFrame
import pragmatic.toKotlinList

class PragmaticTest : SparkTestBase() {

    data class Person(val name: String, val age: Int)

    @Test
    fun `should create and collect dataframe`() {
        val data = listOf(Person("Alice", 30), Person("Bob", 25))
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<Person>()
        assertEquals(data, results)
    }
}
