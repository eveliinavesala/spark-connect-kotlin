package pragmatic

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PragmaticEnumTest : SparkTestBase() {

    enum class UserStatus {
        ACTIVE, INACTIVE, PENDING
    }

    data class UserWithEnum(
        val id: Int,
        val status: UserStatus
    )

    @Test
    fun `pragmatic approach should handle enums`() {
        val data = listOf(
            UserWithEnum(1, UserStatus.ACTIVE),
            UserWithEnum(2, UserStatus.INACTIVE)
        )

        // This test will fail until the pragmatic implementation is updated
        // to handle enum types correctly.
        val df = spark.createPragmaticDataFrame(data, UserWithEnum::class)
        val results = df.toKotlinList(UserWithEnum::class)

        assertEquals(data, results)
    }
}
