package classes

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import pragmatic.toDataFrame
import pragmatic.toKotlinList

// Define the value class for the test
@JvmInline
value class UserId(val id: Long)

// Use the value class in a data class
data class UserWithId(val id: UserId, val name: String)

class IdiomaticValueClassTest : SparkTestBase() {

    @Test
    fun `should handle data classes with value class properties`() {
        val users = listOf(
            UserWithId(UserId(1L), "Alice"),
            UserWithId(UserId(2L), "Bob")
        )

        val df = users.toDataFrame(spark)
        df.printSchema()

        val results = df.toKotlinList<UserWithId>()

        assertEquals(users, results)
        assertEquals(1L, results.first().id.id)
    }
}
