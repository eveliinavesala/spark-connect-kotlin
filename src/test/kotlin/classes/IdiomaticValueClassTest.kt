package classes

import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

class IdiomaticValueClassTest : SparkTestBase() {

    @Test
    fun `test value class with spark dataframe`() {
        val data = listOf(
            IdiomaticValueClass("A"),
            IdiomaticValueClass("B")
        )
        val df = spark.createDataFrame(data, IdiomaticValueClass::class.java)

        assertEquals(2, df.count())
        assertEquals("A", df.first().getAs<String>("value"))
    }

    @Test
    fun `test value class with spark dataset`() {
        val data = listOf(
            IdiomaticValueClass("A"),
            IdiomaticValueClass("B")
        )
        val ds = spark.createDataset(data, Encoders.bean(IdiomaticValueClass::class.java))

        assertEquals(2, ds.count())
        assertEquals("A", ds.first().value)
    }
}
