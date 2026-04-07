package spark.kotlin.reflect

import org.apache.spark.sql.types.StructType
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KType

internal object ReflectionCache {
    // Cache keys changed from KClass to KType to support generics (e.g. Pair<String, Int> vs Pair<Int, Int>)
    private val schemaCache = ConcurrentHashMap<KType, StructType>()
    private val serializerCache = ConcurrentHashMap<KType, RowSerializer>()
    private val deserializerCache = ConcurrentHashMap<KType, RowDeserializer<*>>()

    fun getSchema(kType: KType): StructType = schemaCache.getOrPut(kType) { inferSchemaInternal(kType) }

    // When a schema is provided, the serializer is not cached globally to avoid conflicts when the schema varies.
    // A composite key (KType + Schema) could be used instead; currently the serializer is created on demand.
    fun getSerializer(kType: KType, schema: StructType? = null): RowSerializer {
        return if (schema != null) {
            RowSerializer.create(kType, schema)
        } else {
            serializerCache.getOrPut(kType) { RowSerializer.create(kType) }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getDeserializer(kType: KType): RowDeserializer<T> = deserializerCache.getOrPut(kType) { RowDeserializer.create<T>(kType) } as RowDeserializer<T>
}
