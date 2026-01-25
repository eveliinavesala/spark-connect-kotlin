package encoder.kotlin

import kotlinx.serialization.KSerializer
import org.apache.spark.sql.Row

/**
 * Deserializer that converts Spark Rows to Kotlin objects.
 *
 * This class acts as a bridge between Spark and kotlinx.serialization,
 * using SparkRowDecoder to perform the actual decoding.
 */
internal class SparkDeserializer<T>(
    private val serializer: KSerializer<T>
) {
    /**
     * Deserialize a Spark Row to a Kotlin object.
     *
     * @param row The Spark Row to deserialize
     * @return The deserialized Kotlin object
     */
    fun deserialize(row: Row): T {
        val decoder = SparkRowDecoder(row)
        return decoder.decodeSerializableValue(serializer)
    }
}
