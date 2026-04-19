package spark.kotlin.reflect

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import kotlin.reflect.KType
import kotlin.reflect.full.createType
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
inline fun <reified T : Any> List<T>.toDataFrame(
    spark: SparkSession,
    schema: StructType? = null,
): Dataset<Row> = spark.createDataFrameFromKotlinList(this, typeOf<T>(), schema)

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
inline fun <reified T : Any> Dataset<Row>.toKotlinList(): List<T> = this.toKotlinListFromDataFrame(typeOf<T>())

/**
 * Non-inline entry point for [toDataFrame] when the [KType] is already available at the call site.
 *
 * Delegates to [createDataFrameViaReflectionInternal]. Prefer the inline [toDataFrame] extension
 * when [T] is a reified type parameter.
 */
fun <T : Any> SparkSession.createDataFrameFromKotlinList(
    data: List<T>,
    kType: KType,
    schema: StructType? = null,
): Dataset<Row> = createDataFrameViaReflectionInternal(data, kType, schema)

/**
 * Non-inline entry point for [toKotlinList] when the [KType] is already available at the call site.
 *
 * Delegates to [toKotlinClassListInternal]. Prefer the inline [toKotlinList] extension when [T]
 * is a reified type parameter.
 */
fun <T : Any> Dataset<Row>.toKotlinListFromDataFrame(kType: KType): List<T> = toKotlinClassListInternal<T>(kType)

/**
 * Returns the Spark [StructType] inferred from [kType] via [ReflectionCache].
 *
 * The result is cached; subsequent calls for the same [KType] return the cached value.
 * Useful for schema validation, DDL generation, or constructing a DataFrame manually.
 */
fun getSparkSchema(kType: KType): StructType = ReflectionCache.getSchema(kType)

// ============================================================================
// Internal entry points
// ============================================================================

internal fun SparkSession.createDataFrameViaReflectionInternal(
    data: List<Any>,
    kType: KType,
    schema: StructType? = null,
): Dataset<Row> {
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
        // Sealed: each row may be a different subclass — subclass is selected per-row via "_type".
        // Column sets differ across subclasses, so we skip the batch-index optimisation here.
        val typeColumnIndex = this.schema().fieldIndex("_type")
        return collectedRows.map { row ->
            val typeName = row.get(typeColumnIndex) as String
            val subClass =
                kClass.allLeafSubclasses().find { it.simpleName == typeName }
                    ?: error("Unknown subclass type '$typeName'")
            @Suppress("UNCHECKED_CAST")
            ReflectionCache.getDeserializer<Any>(subClass.createType()).deserialize(row) as T
        }
    }

    // Non-sealed: all rows share the same schema — pre-build column indices once per batch.
    val deserializer = ReflectionCache.getDeserializer<T>(kType)
    val columnIndices = deserializer.buildColumnIndexMap(this.schema())
    return collectedRows.map { deserializer.deserialize(it, columnIndices) }
}
