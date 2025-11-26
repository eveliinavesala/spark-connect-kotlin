package classes

class JvmWrappedDataClass(val name: String, val age: Int) {
    override fun toString(): String {
        return "JvmWrappedDataClass(name=$name, age=$age)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as JvmWrappedDataClass

        if (name != other.name) return false
        if (age != other.age) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + age
        return result
    }
}
