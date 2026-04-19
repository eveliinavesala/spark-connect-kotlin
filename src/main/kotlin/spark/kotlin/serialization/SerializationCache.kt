package spark.kotlin.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.SerialDescriptor
import org.apache.spark.sql.types.StructType
import java.util.concurrent.ConcurrentHashMap

/**
 * Thread-safe cache for serialization-derived artefacts.
 *
 * Cache keys are [SerialDescriptor] instances; equality is determined by the descriptor's
 * serial name and structure, so the same annotated class always resolves to the same entry
 * regardless of call site. All maps are [ConcurrentHashMap]-backed for lock-free reads.
 *
 * Three artefact families are maintained:
 * - **schemas** — [StructType] instances inferred by [inferSparkSchema]
 * - **serializers** — [SparkSerializer] instances for encoding Kotlin objects to [org.apache.spark.sql.Row]
 * - **deserializers** — [SparkDeserializer] instances for decoding [org.apache.spark.sql.Row] to Kotlin objects
 *
 * When a caller supplies an explicit [org.apache.spark.sql.types.StructType] override (e.g. from
 * Unity Catalog), [getSparkSerializer] is bypassed and [SparkSerializer] is instantiated directly,
 * because a serializer is bound to a schema at construction time and caching by descriptor alone
 * would produce incorrect results if the same type is used with different schemas.
 */
internal object SerializationCache {
    private val schemaCache = ConcurrentHashMap<SerialDescriptor, StructType>()
    private val serializerCache = ConcurrentHashMap<SerialDescriptor, SparkSerializer<*>>()
    private val deserializerCache = ConcurrentHashMap<SerialDescriptor, SparkDeserializer<*>>()

    /** Returns the cached [StructType] for [serializer], computing it via [inferSparkSchema] on first access. */
    fun getSchema(serializer: KSerializer<*>): StructType =
        schemaCache.getOrPut(serializer.descriptor) {
            inferSparkSchema(serializer.descriptor)
        }

    /**
     * Returns the cached [SparkSerializer] for [serializer].
     *
     * The cached instance uses the inferred schema; if a schema override is required, construct
     * [SparkSerializer] directly and do not use this method.
     */
    fun <T> getSparkSerializer(serializer: KSerializer<T>): SparkSerializer<T> {
        @Suppress("UNCHECKED_CAST")
        return serializerCache.getOrPut(serializer.descriptor) {
            SparkSerializer(serializer, getSchema(serializer))
        } as SparkSerializer<T>
    }

    /** Returns the cached [SparkDeserializer] for [serializer], creating it on first access. */
    fun <T> getSparkDeserializer(serializer: KSerializer<T>): SparkDeserializer<T> {
        @Suppress("UNCHECKED_CAST")
        return deserializerCache.getOrPut(serializer.descriptor) {
            SparkDeserializer(serializer)
        } as SparkDeserializer<T>
    }

    /** Clears all caches. Primarily useful in tests to ensure schema isolation between test cases. */
    fun clearAll() {
        schemaCache.clear()
        serializerCache.clear()
        deserializerCache.clear()
    }
}
