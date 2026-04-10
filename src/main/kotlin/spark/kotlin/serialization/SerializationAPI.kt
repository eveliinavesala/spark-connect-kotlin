package spark.kotlin.serialization

import spark.kotlin.serialization.util.LazySerializableRowList
import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Public API for kotlinx.serialization-based DataFrame conversion.
 *
 * This file contains the main extension functions for converting between
 * Kotlin objects and Spark DataFrames using kotlinx.serialization.
 */

// ============================================================================
// Public API - Extension Functions
// ============================================================================

/**
 * Convert a List<T> to a Spark DataFrame using kotlinx.serialization.
 *
 * @param spark The active [SparkSession].
 * @param schema Optional [StructType] override. When provided, schema inference from the
 *   [@Serializable][kotlinx.serialization.Serializable] descriptor is skipped and the supplied
 *   schema is used directly. Useful for injecting an authoritative schema from Unity Catalog,
 *   controlling field ordering, or hardcoding the schema as a production pattern.
 *
 * Example:
 * ```kotlin
 * @Serializable
 * data class Person(val name: String, val age: Int)
 * val people = listOf(Person("Alice", 30), Person("Bob", 25))
 * val df = people.toSerializableDataFrame(spark)
 * // With hardcoded schema:
 * val df = people.toSerializableDataFrame(spark, schema = catalog.getSchema("people"))
 * ```
 */
inline fun <reified T> List<T>.toSerializableDataFrame(spark: SparkSession, schema: StructType? = null): Dataset<Row> {
    return spark.createDataFrameFromSerializable(this, serializer<T>(), schema)
}

/**
 * Convert a Spark DataFrame to List<T> using kotlinx.serialization.
 *
 * Example:
 * ```kotlin
 * val people = df.toSerializableKotlinList<Person>()
 * ```
 */
inline fun <reified T> Dataset<Row>.toSerializableKotlinList(): List<T> {
    return this.toSerializableKotlinListInternal(serializer<T>())
}

// ============================================================================
// Public API - Direct Functions
// ============================================================================

/**
 * Non-inline version for use when serializer is already available.
 * Creates a DataFrame from a list of objects using the provided serializer.
 *
 * When [schema] is provided, inference from the descriptor is bypassed and the supplied schema
 * drives both the DataFrame schema and the row encoder. The cached serializer is NOT reused
 * in this case because [SparkSerializer] is bound to a specific schema at construction time.
 */
fun <T> SparkSession.createDataFrameFromSerializable(
    data: List<T>,
    serializer: KSerializer<T>,
    schema: StructType? = null
): Dataset<Row> {
    if (data.isEmpty()) return this.emptyDataFrame()

    val resolvedSchema = schema ?: SerializationCache.getSchema(serializer)
    val sparkSerializer = if (schema != null)
        SparkSerializer(serializer, resolvedSchema)
    else
        SerializationCache.getSparkSerializer(serializer)

    return this.createDataFrame(LazySerializableRowList(data, sparkSerializer), resolvedSchema)
}

/**
 * Infer the Spark [StructType] schema from a [KSerializer] without encoding any data.
 *
 * Useful for pre-flight schema comparison: compare the serialization backend's expected schema
 * against a runtime DataFrame schema or a schema pulled from Unity Catalog before committing to
 * an encode/decode operation.
 *
 * Example:
 * ```kotlin
 * val expected = schemaFor(serializer<CustomerV2>())
 * val diffs = SchemaDriftReport.compare(expected, df.schema())
 * ```
 */
fun <T> schemaFor(serializer: KSerializer<T>): StructType = inferSparkSchema(serializer.descriptor)

/**
 * Non-inline version for use when serializer is already available.
 * Converts DataFrame rows back to Kotlin objects using the provided serializer.
 */
fun <T> Dataset<Row>.toSerializableKotlinListInternal(serializer: KSerializer<T>): List<T> {
    val collectedRows: List<Row> = this.collectAsList()
    if (collectedRows.isEmpty()) return emptyList()

    // Pre-build the descriptor → column index map once from the DataFrame schema.
    // All rows share the same schema, so scanning it N times per batch is redundant.
    val dataFrameSchema = this.schema()
    val descriptor = serializer.descriptor
    val columnIndexMap = IntArray(descriptor.elementsCount) { i ->
        try { dataFrameSchema.fieldIndex(descriptor.getElementName(i)) } catch (_: IllegalArgumentException) { -1 }
    }

    val sparkDeserializer = SerializationCache.getSparkDeserializer(serializer)
    return collectedRows.map { sparkDeserializer.deserialize(it, columnIndexMap) }
}
