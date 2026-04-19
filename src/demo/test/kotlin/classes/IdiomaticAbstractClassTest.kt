package classes

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IdiomaticAbstractClassTest {
    class ConcreteClass : IdiomaticAbstractClass() {
        var called = false

        override fun doSomething() {
            called = true
        }
    }

    @Test
    fun `test abstract class`() {
        val concrete = ConcreteClass()
        concrete.doSomething()
        assertTrue(concrete.called)
    }
}
