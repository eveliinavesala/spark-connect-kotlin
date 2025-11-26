package classes

import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IdiomaticSealedInterfaceTest : SparkTestBase() {

    @Test
    fun `test sealed interface with spark dataframe`() {
        val data = listOf(
            IdiomaticResult.Success("Data"),
            IdiomaticResult.Error("An error occurred")
        )

        val df = spark.createDataFrame(data, IdiomaticResult::class.java)
        df.show()

        assertEquals(2, df.count())
        assertEquals("Data", df.first().getAs<String>("data"))
    }

    @Test
    fun `test sealed interface with spark dataset`() {
        val data = listOf(
            IdiomaticResult.Success("Data"),
            IdiomaticResult.Error("An error occurred")
        )

        val ds = spark.createDataset(data, Encoders.bean(IdiomaticResult::class.java))
        ds.show()

        assertEquals(2, ds.count())
        assertEquals("Data", (ds.first() as IdiomaticResult.Success).data)
    }
}
