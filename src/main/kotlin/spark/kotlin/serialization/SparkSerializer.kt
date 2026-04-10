package spark.kotlin.serialization

import kotlinx.serialization.KSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import spark.kotlin.serialization.encoders.SparkRowEncoder

/**
 * Bridges [KSerializer] to a Spark [Row] via [SparkRowEncoder].
 *
 * An instance is bound to a specific [KSerializer] and [StructType] at construction time.
 * [SparkRowEncoder] is created fresh on each [serialize] call; the encoder is stateful and
 * accumulates field values as the descriptor is traversed. The finished row is retrieved via
 * [SparkRowEncoder.getRow] after encoding completes.
 *
 * Instances are obtained through [SerializationCache.getSparkSerializer]. When a non-default
 * schema is supplied to the public API (e.g. from Unity Catalog), a new instance is created
 * directly and not stored in the cache.
 */
internal class SparkSerializer<T>(
    private val serializer: KSerializer<T>,
    private val schema: StructType
) {
    /**
     * Encodes [value] to a [GenericRowWithSchema] bound to [schema].
     */
    fun serialize(value: T): Row {
        val encoder = SparkRowEncoder(schema)
        encoder.encodeSerializableValue(serializer, value)
        return encoder.getRow()
    }
}
