package classes

import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IdiomaticSealedClassTest : SparkTestBase() {

    @Test
    fun `test sealed class with spark dataframe`() {
        val data = listOf(
            Success("Data"),
            Error("An error occurred")
        )

        val df = spark.createDataFrame(data, Result::class.java)
        df.show()

        assertEquals(2, df.count())
        assertEquals("Data", df.first().getAs<String>("data"))
    }

    @Test
    fun `test sealed class with spark dataset`() {
        val data = listOf(
            Success("Data"),
            Error("An error occurred")
        )

        val ds = spark.createDataset(data, Encoders.bean(Result::class.java))
        ds.show()

        assertEquals(2, ds.count())
        assertEquals("Data", (ds.first() as Success).data)
    }
}
