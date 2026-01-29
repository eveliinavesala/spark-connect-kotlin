package serialization

import kotlinx.serialization.KSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import serialization.encoders.SparkRowEncoder

/**
 * Serializer that converts Kotlin objects to Spark Rows.
 *
 * This class acts as a bridge between kotlinx.serialization and Spark,
 * using SparkRowEncoder to perform the actual encoding.
 */
internal class SparkSerializer<T>(
    private val serializer: KSerializer<T>,
    private val schema: StructType
) {
    /**
     * Serialize a Kotlin object to a Spark Row.
     *
     * @param value The Kotlin object to serialize
     * @return A Spark Row containing the serialized data
     */
    fun serialize(value: T): Row {
        val encoder = SparkRowEncoder(schema)
        encoder.encodeSerializableValue(serializer, value)
        return encoder.getRow()
    }
}
