package classes

// Renamed to avoid conflict with the idiomatic version.
// The @JvmOverloads annotation is what makes this "JVM-wrapped" by generating
// multiple overloads for Java clients.
@JvmOverloads
fun printMessageJvm(message: String, prefix: String = "Info") {
    println("[$prefix] $message")
}
