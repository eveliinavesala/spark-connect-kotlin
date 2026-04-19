package classes

import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class IdiomaticDataClassTest : SparkTestBase() {
    @Test
    fun `test data class with spark dataframe`() {
        val data =
            listOf(
                IdiomaticDataClass("Alice", 30),
                IdiomaticDataClass("Bob", 40),
            )
        val df = spark.createDataFrame(data, IdiomaticDataClass::class.java)

        assertEquals(2, df.count())
        assertEquals("Alice", df.first().getAs<String>("name"))
    }

    @Test
    fun `test data class with spark dataset fails with bean encoder`() {
        val data =
            listOf(
                IdiomaticDataClass("Alice", 30),
                IdiomaticDataClass("Bob", 40),
            )

        // This test is expected to fail with a NoSuchMethodException because
        // the default bean encoder requires a no-arg constructor, which the
        // idiomatic data class does not have. This is a key finding.
        assertThrows(Exception::class.java) {
            val ds = spark.createDataset(data, Encoders.bean(IdiomaticDataClass::class.java))
            ds.collect()
        }
    }
}
