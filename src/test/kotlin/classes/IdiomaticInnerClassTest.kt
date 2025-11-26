package classes

import org.junit.jupiter.api.Test

class IdiomaticInnerClassTest {

    @Test
    fun `test inner class`() {
        val outer = IdiomaticOuter()
        val inner = outer.Inner()
        inner.doSomething() // Prints "Hello from outer"
    }
}
