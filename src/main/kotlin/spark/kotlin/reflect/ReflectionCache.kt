package spark.kotlin.reflect

import org.apache.spark.sql.types.StructType
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KType

/**
 * Thread-safe cache for reflection-derived serialization artefacts.
 *
 * Cache keys are [KType] rather than [kotlin.reflect.KClass] to distinguish instantiations
 * of generic types (e.g. `Pair<String, Int>` vs `Pair<Int, Int>`). All maps are
 * [ConcurrentHashMap]-backed for lock-free reads under concurrent access.
 *
 * Three artefact families are maintained:
 * - **schemas** — [StructType] instances inferred by [inferSchemaInternal]
 * - **serializers** — [RowSerializer] instances for encoding Kotlin objects to [org.apache.spark.sql.Row]
 * - **deserializers** — [RowDeserializer] instances for decoding [org.apache.spark.sql.Row] to Kotlin objects
 */
internal object ReflectionCache {
    // Cache keys changed from KClass to KType to support generics (e.g. Pair<String, Int> vs Pair<Int, Int>)
    private val schemaCache = ConcurrentHashMap<KType, StructType>()
    private val serializerCache = ConcurrentHashMap<KType, RowSerializer>()
    private val deserializerCache = ConcurrentHashMap<KType, RowDeserializer<*>>()

    /** Returns the cached [StructType] for [kType], computing it via [inferSchemaInternal] on first access. */
    fun getSchema(kType: KType): StructType = schemaCache.getOrPut(kType) { inferSchemaInternal(kType) }

    /**
     * Returns a [RowSerializer] for [kType].
     *
     * When [schema] is provided the serializer is created on demand and not stored in the global cache,
     * because a serializer is bound to a specific schema at construction time and caching by [KType] alone
     * would produce incorrect results if the same type is used with different schemas.
     */
    fun getSerializer(kType: KType, schema: StructType? = null): RowSerializer {
        return if (schema != null) {
            RowSerializer.create(kType, schema)
        } else {
            serializerCache.getOrPut(kType) { RowSerializer.create(kType) }
        }
    }

    /** Returns the cached [RowDeserializer] for [kType], creating it via [RowDeserializer.create] on first access. */
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getDeserializer(kType: KType): RowDeserializer<T> = deserializerCache.getOrPut(kType) { RowDeserializer.create<T>(kType) } as RowDeserializer<T>
}
