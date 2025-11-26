package classes

class JvmWrappedAnonymousObject {
    private class MyAnonymousObject {
        val message = "Hello"
    }

    fun getAnonymousObject(): Any {
        return MyAnonymousObject()
    }
}
