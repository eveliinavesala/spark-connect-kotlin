package serialization

import kotlinx.serialization.serializer
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.jdk.javaapi.CollectionConverters

/**
 * Unit tests for SparkRowDecoder.kt and related decoders.
 *
 * Tests the decoding of Spark Rows to Kotlin objects.
 */
class SparkRowDecoderTest {

    @Test
    fun `test decode simple data class`() {
        val serializer = serializer<SimplePerson>()
        val schema = inferSparkSchema(serializer.descriptor)

        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", 30), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertEquals(30, result.age)
    }

    @Test
    fun `test decode primitive types`() {
        val serializer = serializer<PrimitiveTypes>()
        val schema = inferSparkSchema(serializer.descriptor)

        val row = GenericRowWithSchema(
            arrayOf<Any?>(
                true,
                42.toByte(),
                1000.toShort(),
                100000,
                10000000000L,
                3.14f,
                2.718,
                "test",
                "X"
            ),
            schema
        )

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals(true, result.boolVal)
        assertEquals(42.toByte(), result.byteVal)
        assertEquals(1000.toShort(), result.shortVal)
        assertEquals(100000, result.intVal)
        assertEquals(10000000000L, result.longVal)
        assertEquals(3.14f, result.floatVal, 0.001f)
        assertEquals(2.718, result.doubleVal, 0.001)
        assertEquals("test", result.stringVal)
        assertEquals('X', result.charVal)
    }

    @Test
    fun `test decode nullable fields with null values`() {
        val serializer = serializer<NullableFields>()
        val schema = inferSparkSchema(serializer.descriptor)

        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", null, null), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertNull(result.age)
        assertNull(result.email)
    }

    @Test
    fun `test decode nullable fields with values`() {
        val serializer = serializer<NullableFields>()
        val schema = inferSparkSchema(serializer.descriptor)

        val row = GenericRowWithSchema(arrayOf<Any?>("Bob", 25, "bob@example.com"), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Bob", result.name)
        Assertions.assertEquals(25, result.age)
        assertEquals("bob@example.com", result.email)
    }

    @Test
    fun `test decode nested struct`() {
        // First create the address schema and row
        val addressSerializer = serializer<Address>()
        val addressSchema = inferSparkSchema(addressSerializer.descriptor)
        val addressRow = GenericRowWithSchema(
            arrayOf<Any?>("123 Main St", "Springfield", "12345"),
            addressSchema
        )

        // Now create the person with address
        val serializer = serializer<PersonWithAddress>()
        val schema = inferSparkSchema(serializer.descriptor)
        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", 30, addressRow), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertEquals(30, result.age)
        assertEquals("123 Main St", result.address.street)
        assertEquals("Springfield", result.address.city)
        assertEquals("12345", result.address.zipCode)
    }

    @Test
    fun `test decode list`() {
        val serializer = serializer<PersonWithList>()
        val schema = inferSparkSchema(serializer.descriptor)

        val foods = listOf("Pizza", "Pasta", "Salad")
        val scalaFoods = CollectionConverters.asScala(foods).toSeq()
        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", scalaFoods), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertEquals(3, result.favoriteFoods.size)
        assertEquals("Pizza", result.favoriteFoods[0])
        assertEquals("Pasta", result.favoriteFoods[1])
        assertEquals("Salad", result.favoriteFoods[2])
    }

    @Test
    fun `test decode empty list`() {
        val serializer = serializer<PersonWithList>()
        val schema = inferSparkSchema(serializer.descriptor)

        val emptyScalaList = CollectionConverters.asScala(emptyList<String>()).toSeq()
        val row = GenericRowWithSchema(arrayOf<Any?>("Bob", emptyScalaList), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Bob", result.name)
        assertEquals(0, result.favoriteFoods.size)
    }

    @Test
    fun `test decode map`() {
        val serializer = serializer<PersonWithMap>()
        val schema = inferSparkSchema(serializer.descriptor)

        val attributes = mapOf("email" to "alice@example.com", "phone" to "555-1234")
        val scalaMap = CollectionConverters.asScala(attributes)
        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", scalaMap), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertEquals(2, result.attributes.size)
        assertEquals("alice@example.com", result.attributes["email"])
        assertEquals("555-1234", result.attributes["phone"])
    }

    @Test
    fun `test decode empty map`() {
        val serializer = serializer<PersonWithMap>()
        val schema = inferSparkSchema(serializer.descriptor)

        val emptyScalaMap = CollectionConverters.asScala(emptyMap<String, String>())
        val row = GenericRowWithSchema(arrayOf<Any?>("Bob", emptyScalaMap), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Bob", result.name)
        assertEquals(0, result.attributes.size)
    }

    @Test
    fun `test decode enum`() {
        val serializer = serializer<PersonWithEnum>()
        val schema = inferSparkSchema(serializer.descriptor)

        val row = GenericRowWithSchema(arrayOf<Any?>("Alice", "ACTIVE"), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals("Alice", result.name)
        assertEquals(Status.ACTIVE, result.status)
    }

    @Test
    fun `test decode sealed class instance`() {
        val serializer = serializer<Animal>()
        val schema = inferSparkSchema(serializer.descriptor)

        // Create a row for a Dog instance with discriminator
        val row = GenericRowWithSchema(
            arrayOf<Any?>("encoder.kotlin.Dog", "Buddy", "Golden Retriever"),
            schema
        )

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertTrue(result is Dog)
        assertEquals("Buddy", result.name)
        assertEquals("Golden Retriever", (result as Dog).breed)
    }

    @Test
    fun `test decode collection types`() {
        val serializer = serializer<CollectionTypes>()
        val schema = inferSparkSchema(serializer.descriptor)

        val tags = listOf("tag1", "tag2", "tag3")
        val scores = mapOf("math" to 95, "science" to 88)
        val numbers = listOf(1, 2, 3, 4, 5)

        val scalaTags = CollectionConverters.asScala(tags).toSeq()
        val scalaScores = CollectionConverters.asScala(scores)
        val scalaNumbers = CollectionConverters.asScala(numbers).toSeq()

        val row = GenericRowWithSchema(arrayOf<Any?>(scalaTags, scalaScores, scalaNumbers), schema)

        val sparkDeserializer = SparkDeserializer(serializer)
        val result = sparkDeserializer.deserialize(row)

        assertEquals(3, result.tags.size)
        assertEquals("tag1", result.tags[0])

        assertEquals(2, result.scores.size)
        Assertions.assertEquals(95, result.scores["math"])
        Assertions.assertEquals(88, result.scores["science"])

        assertEquals(5, result.numbers.size)
        Assertions.assertEquals(1, result.numbers[0])
        Assertions.assertEquals(5, result.numbers[4])
    }

    @Test
    fun `test round-trip encoding and decoding`() {
        val original = PersonWithAddress(
            name = "Charlie",
            age = 35,
            address = Address("789 Elm St", "Boston", "02101")
        )

        val serializer = serializer<PersonWithAddress>()
        val schema = inferSparkSchema(serializer.descriptor)

        // Encode
        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(original)

        // Decode
        val sparkDeserializer = SparkDeserializer(serializer)
        val decoded = sparkDeserializer.deserialize(row)

        assertEquals(original, decoded)
    }

    @Test
    fun `test round-trip with collections`() {
        val original = CollectionTypes(
            tags = listOf("kotlin", "spark", "serialization"),
            scores = mapOf("performance" to 100, "reliability" to 95),
            numbers = listOf(10, 20, 30)
        )

        val serializer = serializer<CollectionTypes>()
        val schema = inferSparkSchema(serializer.descriptor)

        // Encode
        val sparkSerializer = SparkSerializer(serializer, schema)
        val row = sparkSerializer.serialize(original)

        // Decode
        val sparkDeserializer = SparkDeserializer(serializer)
        val decoded = sparkDeserializer.deserialize(row)

        assertEquals(original, decoded)
    }
}
