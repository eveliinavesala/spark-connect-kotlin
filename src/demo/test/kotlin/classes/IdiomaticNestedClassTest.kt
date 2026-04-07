package classes

import org.junit.jupiter.api.Test

class IdiomaticNestedClassTest {

    @Test
    fun `test nested class`() {
        val nested = IdiomaticOuterClass.Nested()
        nested.doSomething() // Prints "Doing something in nested class"
    }
}
