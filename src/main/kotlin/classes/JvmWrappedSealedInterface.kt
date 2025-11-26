package classes

interface JvmWrappedResultInterface

class JvmWrappedSuccessInterface(val data: String) : JvmWrappedResultInterface

class JvmWrappedErrorInterface(val error: String) : JvmWrappedResultInterface
