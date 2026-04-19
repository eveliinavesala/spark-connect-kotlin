package classes

import org.junit.jupiter.api.Test

class IdiomaticFinalClassTest {
    @Test
    fun `test final class`() {
        val instance = IdiomaticFinalClass()
        instance.doSomething() // Prints "Doing something"
    }
}
