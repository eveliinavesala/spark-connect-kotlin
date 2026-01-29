package serialization

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import pragmatic.toDataFrame
import pragmatic.toKotlinList

/**
 * Comparison tests between kotlinx.serialization-based and reflection-based implementations.
 *
 * These tests verify that both implementations produce equivalent results.
 */
class SerializationVsReflectionTest : SparkTestBase() {

    @Test
    fun `test simple person - both implementations produce same results`() {
        val people = listOf(
            SimplePerson("Alice", 30),
            SimplePerson("Bob", 25),
            SimplePerson("Charlie", 35)
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<SimplePerson>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<SimplePerson>()

        // Both should produce same results
        assertEquals(people.size, serializableResult.size)
        assertEquals(people.size, reflectionResult.size)
        assertEquals(serializableResult, reflectionResult)
        assertEquals(people, serializableResult)
        assertEquals(people, reflectionResult)
    }

    @Test
    fun `test nested structures - both implementations handle nesting`() {
        val people = listOf(
            PersonWithAddress("Alice", 30, Address("123 Main St", "NYC", "10001")),
            PersonWithAddress("Bob", 25, Address("456 Oak Ave", "LA", "90001"))
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<PersonWithAddress>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<PersonWithAddress>()

        // Both should preserve nested structures
        assertEquals(people.size, serializableResult.size)
        assertEquals(people.size, reflectionResult.size)

        assertEquals("NYC", serializableResult[0].address.city)
        assertEquals("NYC", reflectionResult[0].address.city)
        assertEquals("LA", serializableResult[1].address.city)
        assertEquals("LA", reflectionResult[1].address.city)
    }

    @Test
    fun `test lists - both implementations handle collections`() {
        val people = listOf(
            PersonWithList("Alice", listOf("Pizza", "Pasta")),
            PersonWithList("Bob", listOf("Burger"))
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<PersonWithList>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<PersonWithList>()

        // Both should preserve lists
        assertEquals(2, serializableResult[0].favoriteFoods.size)
        assertEquals(2, reflectionResult[0].favoriteFoods.size)
        assertEquals(listOf("Pizza", "Pasta"), serializableResult[0].favoriteFoods)
        assertEquals(listOf("Pizza", "Pasta"), reflectionResult[0].favoriteFoods)
    }

    @Test
    fun `test maps - both implementations handle map types`() {
        val people = listOf(
            PersonWithMap("Alice", mapOf("email" to "alice@example.com", "phone" to "555-1234")),
            PersonWithMap("Bob", mapOf("email" to "bob@example.com"))
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<PersonWithMap>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<PersonWithMap>()

        // Both should preserve maps
        assertEquals(2, serializableResult[0].attributes.size)
        assertEquals(2, reflectionResult[0].attributes.size)
        assertEquals("alice@example.com", serializableResult[0].attributes["email"])
        assertEquals("alice@example.com", reflectionResult[0].attributes["email"])
    }

    @Test
    fun `test enums - both implementations handle enums`() {
        val people = listOf(
            PersonWithEnum("Alice", Status.ACTIVE),
            PersonWithEnum("Bob", Status.INACTIVE)
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<PersonWithEnum>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<PersonWithEnum>()

        // Both should preserve enum values
        assertEquals(Status.ACTIVE, serializableResult[0].status)
        assertEquals(Status.ACTIVE, reflectionResult[0].status)
        assertEquals(Status.INACTIVE, serializableResult[1].status)
        assertEquals(Status.INACTIVE, reflectionResult[1].status)
    }

    @Test
    fun `test sealed classes - both implementations handle polymorphism`() {
        val animals: List<Animal> = listOf(
            Dog("Buddy", "Golden Retriever"),
            Cat("Whiskers", "Orange"),
            Bird("Tweety", true)
        )

        // Using kotlinx.serialization
        val serializableDf = animals.toSerializableDataFrame(spark)
        serializableDf.show(false)
        serializableDf.printSchema()
        val serializableResult = serializableDf.toSerializableKotlinList<Animal>()

        // Using reflection
        val reflectionDf = animals.toDataFrame(spark)
        reflectionDf.show(false)
        reflectionDf.printSchema()
        val reflectionResult = reflectionDf.toKotlinList<Animal>()

        // Both should preserve polymorphic types
        assertEquals(3, serializableResult.size)
        assertEquals(3, reflectionResult.size)

        assertTrue(serializableResult[0] is Dog)
        assertTrue(reflectionResult[0] is Dog)
        assertTrue(serializableResult[1] is Cat)
        assertTrue(reflectionResult[1] is Cat)

        assertEquals("Buddy", serializableResult[0].name)
        assertEquals("Buddy", reflectionResult[0].name)
    }

    @Test
    fun `test nullable fields - both implementations handle nulls`() {
        val people = listOf(
            NullableFields("Alice", 30, "alice@example.com"),
            NullableFields("Bob", null, null),
            NullableFields("Charlie", 35, null)
        )

        // Using kotlinx.serialization
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<NullableFields>()

        // Using reflection
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<NullableFields>()

        // Both should handle nulls correctly
        assertNotNull(serializableResult[0].age)
        assertNotNull(reflectionResult[0].age)
        assertNull(serializableResult[1].age)
        assertNull(reflectionResult[1].age)
        assertNull(serializableResult[1].email)
        assertNull(reflectionResult[1].email)
    }

    @Test
    fun `test schema compatibility - both produce similar schemas`() {
        val people = listOf(SimplePerson("Alice", 30))

        val serializableDf = people.toSerializableDataFrame(spark)
        val reflectionDf = people.toDataFrame(spark)

        // Both should have same column names
        val serializableColumns = serializableDf.columns().toSet()
        val reflectionColumns = reflectionDf.columns().toSet()

        assertEquals(serializableColumns, reflectionColumns)
    }

    @Test
    fun `test performance comparison - serialization should be competitive`() {
        // Create a medium-sized dataset
        val people = (1..100).map { i ->
            SimplePerson("Person$i", 20 + (i % 50))
        }

        // Measure kotlinx.serialization
        val serializableStart = System.currentTimeMillis()
        val serializableDf = people.toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<SimplePerson>()
        val serializableTime = System.currentTimeMillis() - serializableStart

        // Measure reflection
        val reflectionStart = System.currentTimeMillis()
        val reflectionDf = people.toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<SimplePerson>()
        val reflectionTime = System.currentTimeMillis() - reflectionStart

        println("kotlinx.serialization time: ${serializableTime}ms")
        println("Reflection time: ${reflectionTime}ms")

        // Both should produce correct results
        assertEquals(100, serializableResult.size)
        assertEquals(100, reflectionResult.size)
        assertEquals(people, serializableResult)
        assertEquals(people, reflectionResult)
    }

    @Test
    fun `test complex nested structures - both implementations`() {
        val company = Company(
            name = "TechCorp",
            departments = listOf(
                Department(
                    name = "Engineering",
                    employees = listOf(
                        Employee("Alice", 100000.0, Address("123 Main", "NYC", "10001")),
                        Employee("Bob", 95000.0, Address("456 Oak", "NYC", "10002"))
                    )
                )
            )
        )

        // Using kotlinx.serialization
        val serializableDf = listOf(company).toSerializableDataFrame(spark)
        val serializableResult = serializableDf.toSerializableKotlinList<Company>()

        // Using reflection
        val reflectionDf = listOf(company).toDataFrame(spark)
        val reflectionResult = reflectionDf.toKotlinList<Company>()

        // Both should handle deep nesting
        assertEquals("TechCorp", serializableResult[0].name)
        assertEquals("TechCorp", reflectionResult[0].name)
        assertEquals(1, serializableResult[0].departments.size)
        assertEquals(1, reflectionResult[0].departments.size)
        assertEquals("Engineering", serializableResult[0].departments[0].name)
        assertEquals("Engineering", reflectionResult[0].departments[0].name)
        assertEquals(2, serializableResult[0].departments[0].employees.size)
        assertEquals(2, reflectionResult[0].departments[0].employees.size)
    }
}
