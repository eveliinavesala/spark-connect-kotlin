package encoder.kotlin

import kotlinx.serialization.KSerializer
import kotlinx.serialization.serializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

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
 * Example:
 * ```kotlin
 * @Serializable
 * data class Person(val name: String, val age: Int)
 * val people = listOf(Person("Alice", 30), Person("Bob", 25))
 * val df = people.toSerializableDataFrame(spark)
 * ```
 */
inline fun <reified T> List<T>.toSerializableDataFrame(spark: SparkSession): Dataset<Row> {
    return spark.createDataFrameFromSerializable(this, serializer<T>())
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
 */
fun <T> SparkSession.createDataFrameFromSerializable(data: List<T>, serializer: KSerializer<T>): Dataset<Row> {
    if (data.isEmpty()) return this.emptyDataFrame()

    val schema = SerializationCache.getSchema(serializer)
    val sparkSerializer = SerializationCache.getSparkSerializer(serializer)

    return this.createDataFrame(LazySerializableRowList(data, sparkSerializer), schema)
}

/**
 * Non-inline version for use when serializer is already available.
 * Converts DataFrame rows back to Kotlin objects using the provided serializer.
 */
fun <T> Dataset<Row>.toSerializableKotlinListInternal(serializer: KSerializer<T>): List<T> {
    val collectedRows: List<Row> = this.collectAsList()
    if (collectedRows.isEmpty()) return emptyList()

    val sparkDeserializer = SerializationCache.getSparkDeserializer(serializer)
    return collectedRows.map { sparkDeserializer.deserialize(it) }
}
