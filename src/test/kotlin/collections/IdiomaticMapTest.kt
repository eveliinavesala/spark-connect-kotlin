package collections

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.map_keys
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import classes.SparkTestBase
import java.util.Arrays

class IdiomaticMapTest : SparkTestBase() {

    @Test
    fun `test map with spark dataframe`() {
        val data = listOf(
            UserWithScores("user1", mapOf("math" to 95, "science" to 88))
        )
        val df = spark.createDataFrame(data, UserWithScores::class.java)

        df.show()

        // Extract and check one of the keys from the map
        val keys = df.select(map_keys(df.col("scores"))).first().getSeq<String>(0)
        assertEquals(2, keys.size())
        assertEquals("math", keys.head())
    }

    @Test
    fun `test map with spark dataset`() {
        val data = listOf(
            UserWithScores("user1", mapOf("math" to 95, "science" to 88))
        )
        val ds = spark.createDataset(data, Encoders.bean(UserWithScores::class.java))

        ds.show()

        val userScores = ds.first().scores
        assertEquals(2, userScores.size)
        assertEquals(95, userScores["math"])
    }
}
