package classes

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals

// Define an interface to provide a specific type for the anonymous object
private interface MessageHolder {
    val message: String
}

private fun getTypedAnonymousObject(): MessageHolder = object : MessageHolder {
    override val message = "Hello"
}

class IdiomaticAnonymousObjectTest {

    @Test
    fun `test anonymous object`() {
        val anonymous = getTypedAnonymousObject()
        assertEquals("Hello", anonymous.message)
    }
}
