package classes

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IdiomaticInterfaceTest {
    class ConcreteImplementation : IdiomaticInterface {
        var called = false

        override fun doSomething() {
            called = true
        }
    }

    @Test
    fun `test interface`() {
        val concrete = ConcreteImplementation()
        concrete.doSomething()
        assertTrue(concrete.called)
    }
}
