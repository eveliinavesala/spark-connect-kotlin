package classes

abstract class JvmWrappedResult

class JvmWrappedSuccess(val data: String) : JvmWrappedResult()

class JvmWrappedError(val error: String) : JvmWrappedResult()
