package classes

class JvmWrappedCompanionObject {
    companion object {
        @JvmStatic
        fun create(): JvmWrappedCompanionObject = JvmWrappedCompanionObject()
    }
}
