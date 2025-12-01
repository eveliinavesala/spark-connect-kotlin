package pragmatic

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import pragmatic.toDataFrame
import pragmatic.toKotlinList

class PragmaticEnumTest : SparkTestBase() {

    enum class UserStatus {
        ACTIVE, INACTIVE, PENDING
    }

    data class UserWithEnum(
        val id: Int,
        val status: UserStatus
    )

    @Test
    fun `should handle enums`() {
        val data = listOf(
            UserWithEnum(1, UserStatus.ACTIVE),
            UserWithEnum(2, UserStatus.INACTIVE)
        )

        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<UserWithEnum>()
        assertEquals(data, results)
    }
}
