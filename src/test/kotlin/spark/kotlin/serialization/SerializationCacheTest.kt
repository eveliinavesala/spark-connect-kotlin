package spark.kotlin.serialization

import kotlinx.serialization.serializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Unit tests for SerializationCache.kt
 *
 * Tests caching behavior for schemas, serializers, and deserializers.
 */
class SerializationCacheTest {
    @Test
    fun `test schema caching returns same instance`() {
        val serializer = serializer<SimplePerson>()

        val schema1 = SerializationCache.getSchema(serializer)
        val schema2 = SerializationCache.getSchema(serializer)

        // Should return the same cached instance
        assertSame(schema1, schema2)
    }

    @Test
    fun `test spark serializer caching returns same instance`() {
        val serializer = serializer<SimplePerson>()

        val sparkSerializer1 = SerializationCache.getSparkSerializer(serializer)
        val sparkSerializer2 = SerializationCache.getSparkSerializer(serializer)

        // Should return the same cached instance
        assertSame(sparkSerializer1, sparkSerializer2)
    }

    @Test
    fun `test spark deserializer caching returns same instance`() {
        val serializer = serializer<SimplePerson>()

        val sparkDeserializer1 = SerializationCache.getSparkDeserializer(serializer)
        val sparkDeserializer2 = SerializationCache.getSparkDeserializer(serializer)

        // Should return the same cached instance
        assertSame(sparkDeserializer1, sparkDeserializer2)
    }

    @Test
    fun `test different types have different cached objects`() {
        val personSerializer = serializer<SimplePerson>()
        val addressSerializer = serializer<Address>()

        val personSchema = SerializationCache.getSchema(personSerializer)
        val addressSchema = SerializationCache.getSchema(addressSerializer)

        // Different types should have different schemas
        assertNotSame(personSchema, addressSchema)
        assertEquals(2, personSchema.fields().size)
        assertEquals(3, addressSchema.fields().size)
    }

    @Test
    fun `test cache works with complex types`() {
        val serializer = serializer<PersonWithAddress>()

        // Get multiple times
        val schema1 = SerializationCache.getSchema(serializer)
        val sparkSerializer1 = SerializationCache.getSparkSerializer(serializer)
        val sparkDeserializer1 = SerializationCache.getSparkDeserializer(serializer)

        val schema2 = SerializationCache.getSchema(serializer)
        val sparkSerializer2 = SerializationCache.getSparkSerializer(serializer)
        val sparkDeserializer2 = SerializationCache.getSparkDeserializer(serializer)

        // All should be cached
        assertSame(schema1, schema2)
        assertSame(sparkSerializer1, sparkSerializer2)
        assertSame(sparkDeserializer1, sparkDeserializer2)
    }

    @Test
    fun `test cache with sealed classes`() {
        val serializer = serializer<Animal>()

        val schema1 = SerializationCache.getSchema(serializer)
        val schema2 = SerializationCache.getSchema(serializer)

        assertSame(schema1, schema2)

        // Verify sealed class schema has discriminator
        val fieldNames = schema1.fields().map { it.name() }
        assertTrue(fieldNames.contains("_type"))
    }

    @Test
    fun `test cache with collection types`() {
        val serializer = serializer<CollectionTypes>()

        val sparkSerializer1 = SerializationCache.getSparkSerializer(serializer)
        val sparkSerializer2 = SerializationCache.getSparkSerializer(serializer)

        assertSame(sparkSerializer1, sparkSerializer2)
    }

    @Test
    fun `test cached objects are functional`() {
        val serializer = serializer<SimplePerson>()
        val sparkSerializer = SerializationCache.getSparkSerializer(serializer)
        val sparkDeserializer = SerializationCache.getSparkDeserializer(serializer)

        // Use cached objects for round-trip
        val original = SimplePerson("Alice", 30)
        val row = sparkSerializer.serialize(original)
        val decoded = sparkDeserializer.deserialize(row)

        assertEquals(original, decoded)
    }

    @Test
    fun `test cache thread safety simulation`() {
        val serializer = serializer<SimplePerson>()

        // Simulate concurrent access by getting from multiple "threads"
        val schemas =
            (1..10).map {
                SerializationCache.getSchema(serializer)
            }

        // All should be the same instance
        val first = schemas.first()
        assertTrue(schemas.all { it === first })
    }
}
