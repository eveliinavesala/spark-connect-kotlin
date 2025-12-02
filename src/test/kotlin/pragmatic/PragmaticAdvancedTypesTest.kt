package pragmatic

import classes.SparkTestBase
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import org.junit.jupiter.api.Assertions.assertEquals
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
        df.show()
        val results = df.toKotlinList<ApiResponse>()
        assertEquals(data, results)
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
        df.show()
        val results = df.toKotlinList<Event>()
        assertEquals(data, results)
    }
}
