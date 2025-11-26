package classes

// In Kotlin, a nested class is equivalent to a static nested class in Java.
// It does not have a reference to its outer class.
class JvmWrappedOuterClass {
    class Nested {
        fun doSomething() {
            println("Doing something in nested class")
        }
    }
}
