package pragmatic

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PragmaticNestedTest : SparkTestBase() {

    // --- Test Data Classes for Level 1: Simple Nesting ---

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

        // This test will fail because the current pragmatic implementation
        // does not handle nested data classes. This is the expected failure.
        val df = spark.createPragmaticDataFrame(data, SimpleUser::class)
        val results = df.toKotlinList(SimpleUser::class)

        assertEquals(data, results)
    }
}
