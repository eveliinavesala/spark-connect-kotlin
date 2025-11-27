package collections

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.size
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import classes.SparkTestBase

class IdiomaticSetTest : SparkTestBase() {

    @Test
    fun `test set with spark dataframe`() {
        val data = listOf(
            PersonWithSet("Charlie", setOf("USA", "Canada", "USA"))
        )
        val df = spark.createDataFrame(data, PersonWithSet::class.java)

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
        val ds = spark.createDataset(data, Encoders.bean(PersonWithSet::class.java))

        ds.show()

        val countries = ds.first().visitedCountries
        assertEquals(2, countries.size)
        assertTrue(countries.contains("USA"))
        assertTrue(countries.contains("Canada"))
    }
}
