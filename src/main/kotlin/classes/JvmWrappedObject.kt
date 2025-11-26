package classes

class JvmWrappedObject private constructor() {
    companion object {
        val INSTANCE = JvmWrappedObject()
    }

    fun log(message: String) {
        println(message)
    }
}
