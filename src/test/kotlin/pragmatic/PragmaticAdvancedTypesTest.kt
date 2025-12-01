package pragmatic

import classes.SparkTestBase
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
    fun `pragmatic approach should handle simple nested data classes`() {
        val data = listOf(
            SimpleUser(
                id = 1L,
                username = "alice",
                profile = SimpleProfile("alice@example.com", "alice.dev")
            )
        )
        val df = spark.createPragmaticDataFrame(data, SimpleUser::class)
        val results = df.toKotlinList(SimpleUser::class)
        assertEquals(data, results)
    }

    // --- Test for Sealed Classes ---

    sealed class ApiResponse {
        data class Success(val data: String) : ApiResponse()
        data class Error(val errorCode: Int, val message: String) : ApiResponse()
    }

    @Test
    fun `pragmatic approach should handle sealed classes`() {
        val data = listOf(
            ApiResponse.Success("Data for request 1"),
            ApiResponse.Error(404, "Not Found"),
            ApiResponse.Success("More data")
        )
        val df = spark.createPragmaticDataFrame(data, ApiResponse::class)
        df.show()
        val results = df.toKotlinList(ApiResponse::class)
        assertEquals(data, results)
    }
}
