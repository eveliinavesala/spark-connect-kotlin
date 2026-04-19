package collections

import classes.SparkTestBase
import org.apache.spark.sql.functions.size
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

class IdiomaticListTest : SparkTestBase() {
    @Test
    fun `test list with spark dataframe`() {
        val data =
            listOf(
                PersonWithList("Alice", listOf("Pizza", "Pasta")),
                PersonWithList("Bob", listOf("Burger")),
            )
        val df = data.toDataFrame(spark)

        df.show()

        // Check the size of the array column
        val aliceFoods =
            df
                .filter("name = 'Alice'")
                .select(size(df.col("favoriteFoods")))
                .first()
                .getInt(0)
        assertEquals(2, aliceFoods)
    }

    @Test
    fun `test list with spark dataset`() {
        val data =
            listOf(
                PersonWithList("Alice", listOf("Pizza", "Pasta")),
                PersonWithList("Bob", listOf("Burger")),
            )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<PersonWithList>()

        results.forEach { println(it) }

        val bobFoods = results.filter { it.name == "Bob" }.first().favoriteFoods
        assertEquals(1, bobFoods.size)
        assertEquals("Burger", bobFoods[0])
    }
}
