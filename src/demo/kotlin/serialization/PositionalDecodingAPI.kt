package serialization

import kotlinx.serialization.serializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import serialization.decoders.PositionalSparkRowDecoder

inline fun <reified T> Dataset<Row>.toPositionalKotlinList(): List<T> {
    val kSerializer = serializer<T>()
    return collectAsList().map { row -> kSerializer.deserialize(PositionalSparkRowDecoder(row)) }
}
