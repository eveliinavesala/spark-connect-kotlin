package encoder.kotlin

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.serialization.Serializable

/**
 * Test data models for kotlinx.serialization-based encoder/decoder tests.
 */

// Basic data class
@Serializable
data class SimplePerson(
    val name: String,
    val age: Int
)

// Data class with various primitive types
@Serializable
data class PrimitiveTypes(
    val boolVal: Boolean,
    val byteVal: Byte,
    val shortVal: Short,
    val intVal: Int,
    val longVal: Long,
    val floatVal: Float,
    val doubleVal: Double,
    val stringVal: String,
    val charVal: Char
)

// Data class with nullable fields
@Serializable
data class NullableFields(
    val name: String,
    val age: Int?,
    val email: String?
)

// Nested data class
@Serializable
data class Address(
    val street: String,
    val city: String,
    val zipCode: String
)

@Serializable
data class PersonWithAddress(
    val name: String,
    val age: Int,
    val address: Address
)

// Data class with collections
@Serializable
data class PersonWithList(
    val name: String,
    val favoriteFoods: List<String>
)

@Serializable
data class PersonWithMap(
    val name: String,
    val attributes: Map<String, String>
)

@Serializable
data class CollectionTypes(
    val tags: List<String>,
    val scores: Map<String, Int>,
    val numbers: List<Int>
)

// Enum
@Serializable
enum class Status {
    ACTIVE,
    INACTIVE,
    PENDING
}

@Serializable
data class PersonWithEnum(
    val name: String,
    val status: Status
)

// Sealed class (polymorphism)
@Serializable
sealed class Animal {
    abstract val name: String
}

@Serializable
data class Dog(override val name: String, val breed: String) : Animal()

@Serializable
data class Cat(override val name: String, val color: String) : Animal()

@Serializable
data class Bird(override val name: String, val canFly: Boolean) : Animal()

@Serializable
data class Zoo(
    val location: String,
    val animals: List<Animal>
)

// Date/Time types
@Serializable
data class EventWithDates(
    val title: String,
    val eventDate: LocalDate,
    val timestamp: Instant
)

// Deeply nested structure
@Serializable
data class Company(
    val name: String,
    val departments: List<Department>
)

@Serializable
data class Department(
    val name: String,
    val employees: List<Employee>
)

@Serializable
data class Employee(
    val name: String,
    val salary: Double,
    val address: Address
)

// Complex nested with collections and polymorphism
@Serializable
data class ComplexData(
    val id: String,
    val metadata: Map<String, String>,
    val items: List<DataItem>
)

@Serializable
sealed class DataItem {
    abstract val id: Int
}

@Serializable
data class TextItem(override val id: Int, val text: String) : DataItem()

@Serializable
data class NumberItem(override val id: Int, val value: Double) : DataItem()
