package classes

// The `inner` keyword in Kotlin makes a nested class an inner class,
// giving it access to the members of its outer class. This is the
// direct equivalent of a non-static inner class in Java.
class JvmWrappedOuter {
    private val message = "Hello from outer"

    inner class Inner {
        fun doSomething() {
            println(message)
        }
    }
}
