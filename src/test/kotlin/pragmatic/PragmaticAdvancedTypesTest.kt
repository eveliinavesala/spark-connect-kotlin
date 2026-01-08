package pragmatic

import classes.SparkTestBase
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PragmaticAdvancedTypesTest : SparkTestBase() {

    // --- Test for Nested Data Classes ---

    data class SimpleProfile(
        val email: String,
        val website: String?
    )

    data class SimpleUser(
        val id: Long,
        val username: String,
        val profile: SimpleProfile
    )

    @Test
    fun `should handle simple nested data classes`() {
        val data = listOf(
            SimpleUser(
                id = 1L,
                username = "alice",
                profile = SimpleProfile("alice@example.com", "alice.dev")
            )
        )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<SimpleUser>()
        assertEquals(data, results)
    }

    // --- Test for Sealed Classes ---

    sealed class ApiResponse {
        data class Success(val data: String) : ApiResponse()
        data class Error(val errorCode: Int, val message: String) : ApiResponse()
    }

    @Test
    fun `should handle sealed classes`() {
        val data = listOf(
            ApiResponse.Success("Data for request 1"),
            ApiResponse.Error(404, "Not Found"),
            ApiResponse.Success("More data")
        )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<ApiResponse>()

        assertEquals(data.size, results.size, "Result list size should match original data size")

        // Assert each element individually
        assertTrue(results[0] is ApiResponse.Success, "First element should be ApiResponse.Success")
        assertEquals("Data for request 1", (results[0] as ApiResponse.Success).data, "First success data should match")

        assertTrue(results[1] is ApiResponse.Error, "Second element should be ApiResponse.Error")
        assertEquals(404, (results[1] as ApiResponse.Error).errorCode, "Error code should match")
        assertEquals("Not Found", (results[1] as ApiResponse.Error).message, "Error message should match")

        assertTrue(results[2] is ApiResponse.Success, "Third element should be ApiResponse.Success")
        assertEquals("More data", (results[2] as ApiResponse.Success).data, "Second success data should match")
    }

    // --- Test for kotlinx-datetime ---

    data class Event(
        val name: String,
        val date: LocalDate,
        val timestamp: Instant
    )

    @Test
    fun `should handle kotlinx-datetime types`() {
        val data = listOf(
            Event(
                name = "KotlinConf 2024",
                date = LocalDate(2024, 5, 22),
                timestamp = Instant.parse("2024-05-22T10:00:00Z")
            )
        )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<Event>()
        assertEquals(data, results)
    }
}
