package classes

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

// Define the sealed interface and its subclasses within the test file for clarity
sealed interface IdiomaticResult {
    data class Success(
        val data: String,
    ) : IdiomaticResult

    data class Error(
        val message: String,
    ) : IdiomaticResult
}

class IdiomaticSealedInterfaceTest : SparkTestBase() {
    @Test
    fun `test sealed interface with spark dataframe`() {
        val data =
            listOf(
                IdiomaticResult.Success("Data"),
                IdiomaticResult.Error("An error occurred"),
            )

        val df = data.toDataFrame(spark)

        assertEquals(2, df.count())
        val successRow = df.filter("`_type` = 'Success'").first()
        assertEquals("Data", successRow.getAs<String>("data"))
    }

    @Test
    fun `test sealed interface with spark dataset`() {
        val data =
            listOf(
                IdiomaticResult.Success("Data"),
                IdiomaticResult.Error("An error occurred"),
            )

        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<IdiomaticResult>()

        assertEquals(2, results.size)
        val firstResult = results.first()
        assertTrue(firstResult is IdiomaticResult.Success, "First element should be IdiomaticResult.Success")
        assertEquals("Data", (firstResult as IdiomaticResult.Success).data)

        val secondResult = results[1]
        assertTrue(secondResult is IdiomaticResult.Error, "Second element should be IdiomaticResult.Error")
        assertEquals("An error occurred", (secondResult as IdiomaticResult.Error).message)
    }
}
