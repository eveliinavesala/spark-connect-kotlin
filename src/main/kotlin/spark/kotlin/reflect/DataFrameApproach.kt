package spark.kotlin.reflect

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

// ============================================================================
// Public API
// ============================================================================

/**
 * Converts this list of Kotlin objects into a Spark [Dataset]<[Row]> using reflection-based
 * schema inference.
 *
 * The schema is inferred from [T]'s primary constructor at the first call for a given type and
 * cached for all subsequent calls. Supported types include primitives, Strings, data classes
 * (nested), sealed classes, enums, value classes, nullable fields, and generic collections
 * (List, Set, Map, Pair, Triple).
 *
 * @param spark The active [SparkSession].
 * @param schema Optional [StructType] override. When provided, schema inference is skipped and
 *   the supplied schema is used directly. Useful for controlling nullability, metadata,
 *   or field ordering explicitly.
 * @return A [Dataset]<[Row]> whose schema matches [T] (or the supplied [schema]).
 */
inline fun <reified T : Any> List<T>.toDataFrame(spark: SparkSession, schema: StructType? = null): Dataset<Row> {
    return spark.createDataFrameFromKotlinList(this, typeOf<T>(), schema)
}

/**
 * Deserializes each [Row] in this [Dataset] into an instance of [T] using reflection.
 *
 * [T] must have a primary constructor whose parameter names match the column names in the
 * DataFrame schema. Column values are mapped to constructor parameters by name; missing or
 * null values for non-nullable parameters will throw [IllegalArgumentException].
 *
 * For sealed classes, the `_type` discriminator column is used to select the correct subclass
 * before deserialization.
 *
 * @return A [List]<[T]> with one element per row in the dataset.
 */
inline fun <reified T : Any> Dataset<Row>.toKotlinList(): List<T> {
    return this.toKotlinListFromDataFrame(typeOf<T>())
}

fun <T : Any> SparkSession.createDataFrameFromKotlinList(data: List<T>, kType: KType, schema: StructType? = null): Dataset<Row> {
    return createDataFrameViaReflectionInternal(data, kType, schema)
}

fun <T : Any> Dataset<Row>.toKotlinListFromDataFrame(kType: KType): List<T> {
    return toKotlinClassListInternal<T>(kType)
}

/**
 * Returns the Spark StructType schema inferred from a given Kotlin type.
 * Useful for schema validation, DDL generation, or manual DataFrame creation.
 */
fun getSparkSchema(kType: KType): StructType {
    return ReflectionCache.getSchema(kType)
}

// ============================================================================
// Internal entry points
// ============================================================================

internal fun SparkSession.createDataFrameViaReflectionInternal(data: List<Any>, kType: KType, schema: StructType? = null): Dataset<Row> {
    val resolvedSchema = schema ?: ReflectionCache.getSchema(kType)
    if (data.isEmpty()) {
        return this.createDataFrame(emptyList<Row>(), resolvedSchema)
    }
    val serializer = ReflectionCache.getSerializer(kType, resolvedSchema)
    return this.createDataFrame(LazyRowList(data, serializer), resolvedSchema)
}

internal fun <T : Any> Dataset<Row>.toKotlinClassListInternal(kType: KType): List<T> {
    val collectedRows: List<Row> = this.collectAsList()
    val kClass = kType.jvmErasure

    if (kClass.isSealed) {
        return collectedRows.map { row ->
            val typeName = row.getAs<String>("_type")
            val subClass = kClass.allLeafSubclasses().find { it.simpleName == typeName } ?: error("Unknown subclass type '$typeName'")
            // Note: Sealed subclasses are assumed non-generic here; generic sealed subclasses would require additional type argument threading
            @Suppress("UNCHECKED_CAST")
            ReflectionCache.getDeserializer<Any>(subClass.createType()).deserialize(row) as T
        }
    }
    val deserializer = ReflectionCache.getDeserializer<T>(kType)
    return collectedRows.map { deserializer.deserialize(it) }
}
