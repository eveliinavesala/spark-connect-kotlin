package classes

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertTrue

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
