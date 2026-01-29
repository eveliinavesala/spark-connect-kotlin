package serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.spark.sql.types.StructType
import java.util.concurrent.ConcurrentHashMap

/**
 * Cache for serialization-related objects to improve performance.
 *
 * This cache stores:
 * - Spark schemas derived from SerialDescriptors
 * - SparkSerializer instances for encoding
 * - SparkDeserializer instances for decoding
 *
 * All caches use SerialDescriptor as the key to ensure proper cache hits
 * for the same type across different calls.
 */
internal object SerializationCache {

    private val schemaCache = ConcurrentHashMap<SerialDescriptor, StructType>()
    private val serializerCache = ConcurrentHashMap<SerialDescriptor, SparkSerializer<*>>()
    private val deserializerCache = ConcurrentHashMap<SerialDescriptor, SparkDeserializer<*>>()

    /**
     * Get or compute the Spark schema for a given serializer.
     */
    fun getSchema(serializer: KSerializer<*>): StructType {
        return schemaCache.getOrPut(serializer.descriptor) {
            inferSparkSchema(serializer.descriptor)
        }
    }

    /**
     * Get or create a SparkSerializer for encoding objects to Spark Rows.
     */
    fun <T> getSparkSerializer(serializer: KSerializer<T>): SparkSerializer<T> {
        @Suppress("UNCHECKED_CAST")
        return serializerCache.getOrPut(serializer.descriptor) {
            SparkSerializer(serializer, getSchema(serializer))
        } as SparkSerializer<T>
    }

    /**
     * Get or create a SparkDeserializer for decoding Spark Rows to objects.
     */
    fun <T> getSparkDeserializer(serializer: KSerializer<T>): SparkDeserializer<T> {
        @Suppress("UNCHECKED_CAST")
        return deserializerCache.getOrPut(serializer.descriptor) {
            SparkDeserializer(serializer)
        } as SparkDeserializer<T>
    }

    /**
     * Clear all caches. Useful for testing or memory management.
     */
    fun clearAll() {
        schemaCache.clear()
        serializerCache.clear()
        deserializerCache.clear()
    }
}
