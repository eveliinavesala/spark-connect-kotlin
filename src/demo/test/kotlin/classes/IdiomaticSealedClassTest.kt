package classes

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

// Define the sealed class and its subclasses within the test file for clarity
sealed class Result {
    data class Success(
        val data: String,
    ) : Result()

    data class Error(
        val message: String,
    ) : Result()
}

class IdiomaticSealedClassTest : SparkTestBase() {
    @Test
    fun `test sealed class with spark dataframe`() {
        val data =
            listOf(
                Result.Success("Data"),
                Result.Error("An error occurred"),
            )

        val df = data.toDataFrame(spark)

        assertEquals(2, df.count())
        // For sealed classes, the original data class properties are directly accessible
        // The _type field is internal to the conversion
        val successRow = df.filter("`_type` = 'Success'").first()
        assertEquals("Data", successRow.getAs<String>("data"))
    }

    @Test
    fun `test sealed class with spark dataset`() {
        val data =
            listOf(
                Result.Success("Data"),
                Result.Error("An error occurred"),
            )

        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<Result>()

        assertEquals(2, results.size)
        val firstResult = results.first()
        assertTrue(firstResult is Result.Success, "First element should be Result.Success")
        assertEquals("Data", (firstResult as Result.Success).data)

        val secondResult = results[1]
        assertTrue(secondResult is Result.Error, "Second element should be Result.Error")
        assertEquals("An error occurred", (secondResult as Result.Error).message)
    }
}
