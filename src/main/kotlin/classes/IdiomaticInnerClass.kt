package classes

class IdiomaticOuter {
    private val message = "Hello from outer"

    inner class Inner {
        fun doSomething() {
            println(message)
        }
    }
}
