package collections

import org.apache.spark.sql.functions.size
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import classes.SparkTestBase
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

class IdiomaticSetTest : SparkTestBase() {

    @Test
    fun `test set with spark dataframe`() {
        val data = listOf(
            PersonWithSet("Charlie", setOf("USA", "Canada", "USA"))
        )
        val df = data.toDataFrame(spark)

        df.show()

        // A Set automatically handles duplicates, so the size should be 2
        val countryCount = df.select(size(df.col("visitedCountries"))).first().getInt(0)
        assertEquals(2, countryCount)
    }

    @Test
    fun `test set with spark dataset`() {
        val data = listOf(
            PersonWithSet("Charlie", setOf("USA", "Canada", "USA"))
        )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<PersonWithSet>()

        results.forEach { println(it) }

        val countries = results.first().visitedCountries
        assertEquals(2, countries.size)
        assertTrue(countries.contains("USA"))
        assertTrue(countries.contains("Canada"))
    }
}
