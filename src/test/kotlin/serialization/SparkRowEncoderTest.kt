package serialization

import kotlinx.serialization.serializer
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/**
 * Unit tests for SparkRowEncoder.kt and related encoders.
 *
 * Tests the encoding of Kotlin objects to Spark Rows.
 */
class SparkRowEncoderTest {

    @Test
    fun `test encode simple data class`() {
        val person = SimplePerson("Alice", 30)
        val serializer = serializer<SimplePerson>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Alice", row.getString(0))
        assertEquals(30, row.getInt(1))
    }

    @Test
    fun `test encode primitive types`() {
        val data = PrimitiveTypes(
            boolVal = true,
            byteVal = 42.toByte(),
            shortVal = 1000.toShort(),
            intVal = 100000,
            longVal = 10000000000L,
            floatVal = 3.14f,
            doubleVal = 2.718,
            stringVal = "test",
            charVal = 'X'
        )
        val serializer = serializer<PrimitiveTypes>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(data)

        assertEquals(true, row.getBoolean(0))
        assertEquals(42.toByte(), row.getByte(1))
        assertEquals(1000.toShort(), row.getShort(2))
        assertEquals(100000, row.getInt(3))
        assertEquals(10000000000L, row.getLong(4))
        assertEquals(3.14f, row.getFloat(5), 0.001f)
        assertEquals(2.718, row.getDouble(6), 0.001)
        assertEquals("test", row.getString(7))
        assertEquals("X", row.getString(8)) // char becomes string
    }

    @Test
    fun `test encode nullable fields with null values`() {
        val data = NullableFields("Alice", null, null)
        val serializer = serializer<NullableFields>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(data)

        assertEquals("Alice", row.getString(0))
        assertTrue(row.isNullAt(1))
        assertTrue(row.isNullAt(2))
    }

    @Test
    fun `test encode nullable fields with values`() {
        val data = NullableFields("Bob", 25, "bob@example.com")
        val serializer = serializer<NullableFields>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(data)

        assertEquals("Bob", row.getString(0))
        assertEquals(25, row.getInt(1))
        assertEquals("bob@example.com", row.getString(2))
    }

    @Test
    fun `test encode nested struct`() {
        val person = PersonWithAddress(
            name = "Alice",
            age = 30,
            address = Address("123 Main St", "Springfield", "12345")
        )
        val serializer = serializer<PersonWithAddress>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Alice", row.getString(0))
        assertEquals(30, row.getInt(1))

        val addressRow = row.getStruct(2)
        assertEquals("123 Main St", addressRow.getString(0))
        assertEquals("Springfield", addressRow.getString(1))
        assertEquals("12345", addressRow.getString(2))
    }

    @Test
    fun `test encode list`() {
        val person = PersonWithList("Alice", listOf("Pizza", "Pasta", "Salad"))
        val serializer = serializer<PersonWithList>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Alice", row.getString(0))

        val foods = row.getList<String>(1)
        assertEquals(3, foods.size)
        assertEquals("Pizza", foods[0])
        assertEquals("Pasta", foods.get(1))
        assertEquals("Salad", foods.get(2))
    }

    @Test
    fun `test encode empty list`() {
        val person = PersonWithList("Bob", emptyList())
        val serializer = serializer<PersonWithList>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Bob", row.getString(0))

        val foods = row.getList<String>(1)
        assertEquals(0, foods.size)
    }

    @Test
    fun `test encode map`() {
        val person = PersonWithMap(
            "Alice",
            mapOf("email" to "alice@example.com", "phone" to "555-1234")
        )
        val serializer = serializer<PersonWithMap>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Alice", row.getString(0))

        val attributes = row.getJavaMap<String, String>(1)
        assertEquals(2, attributes.size)
        assertEquals("alice@example.com", attributes["email"])
        assertEquals("555-1234", attributes["phone"])
    }

    @Test
    fun `test encode empty map`() {
        val person = PersonWithMap("Bob", emptyMap())
        val serializer = serializer<PersonWithMap>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Bob", row.getString(0))

        val attributes = row.getJavaMap<String, String>(1)
        assertEquals(0, attributes.size)
    }

    @Test
    fun `test encode enum`() {
        val person = PersonWithEnum("Alice", Status.ACTIVE)
        val serializer = serializer<PersonWithEnum>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(person)

        assertEquals("Alice", row.getString(0))
        assertEquals("ACTIVE", row.getString(1))
    }

    @Test
    fun `test encode sealed class instances`() {
        val dog = Dog("Buddy", "Golden Retriever")
        val serializer = serializer<Animal>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(dog)

        // Sealed classes should have a "_type" discriminator field
        assertEquals("encoder.kotlin.Dog", row.getString(0))
        assertEquals("Buddy", row.getString(1))
        assertEquals("Golden Retriever", row.getString(2))
    }

    @Test
    fun `test encode complex nested structure`() {
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
        val serializer = serializer<Company>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(company)

        assertEquals("TechCorp", row.getString(0))

        val departments = row.getList<Row>(1)
        assertEquals(1, departments.size)

        val dept = departments.get(0)
        assertEquals("Engineering", dept.getString(0))

        val employees = dept.getList<Row>(1)
        assertEquals(2, employees.size)

        val alice = employees.get(0)
        assertEquals("Alice", alice.getString(0))
        assertEquals(100000.0, alice.getDouble(1), 0.01)

        val aliceAddress = alice.getStruct(2)
        assertEquals("123 Main", aliceAddress.getString(0))
        assertEquals("NYC", aliceAddress.getString(1))
    }

    @Test
    fun `test encode list of different primitive types`() {
        val data = CollectionTypes(
            tags = listOf("tag1", "tag2", "tag3"),
            scores = mapOf("math" to 95, "science" to 88),
            numbers = listOf(1, 2, 3, 4, 5)
        )
        val serializer = serializer<CollectionTypes>()
        val schema = inferSparkSchema(serializer.descriptor)

        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(data)

        val tags = row.getList<String>(0)
        assertEquals(3, tags.size)

        val scores = row.getJavaMap<String, Int>(1)
        Assertions.assertEquals(95, scores["math"])
        Assertions.assertEquals(88, scores["science"])

        val numbers = row.getList<Int>(2)
        assertEquals(5, numbers.size)
        Assertions.assertEquals(1, numbers.get(0))
    }
}
