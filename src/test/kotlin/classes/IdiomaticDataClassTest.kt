package classes

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IdiomaticDataClassTest : SparkTestBase() {

    @Test
    fun `test data class with spark dataframe`() {
        val data = listOf(
            IdiomaticDataClass("Alice", 30),
            IdiomaticDataClass("Bob", 40)
        )
        val df = spark.createDataFrame(data, IdiomaticDataClass::class.java)

        assertEquals(2, df.count())
        assertEquals("Alice", df.first().getAs<String>("name"))
    }

    @Test
    fun `test data class with spark dataset`() {
        val data = listOf(
            IdiomaticDataClass("Alice", 30),
            IdiomaticDataClass("Bob", 40)
        )
        val ds = spark.createDataset(data, Encoders.bean(IdiomaticDataClass::class.java))

        assertEquals(2, ds.count())
        assertEquals("Alice", ds.first().name)
    }
}
