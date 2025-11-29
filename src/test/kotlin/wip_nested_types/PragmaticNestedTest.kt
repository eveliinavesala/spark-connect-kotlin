package wip_nested_types

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import pragmatic.createPragmaticDataFrame
import pragmatic.toKotlinList

/**
 * This test file is part of a work-in-progress investigation into supporting
 * nested and sealed types. These tests are expected to fail and are used to
 * drive the development of a robust solution.
 */
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
    @Disabled("This test is part of a work-in-progress investigation and is expected to fail.")
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
