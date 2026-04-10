package spark.kotlin.serialization

import kotlinx.serialization.KSerializer
import org.apache.spark.sql.Row
import spark.kotlin.serialization.decoders.SparkRowDecoder

/**
 * Bridges [KSerializer] to a Spark [Row] via [SparkRowDecoder].
 *
 * Decoding is column-name-resolved rather than positional: the decoder maps each descriptor
 * element to its column by name, tolerating column reordering between the schema and the
 * descriptor. Missing columns are skipped and treated as absent (null for nullable fields).
 *
 * Two decode paths are available:
 * - **Per-row** (default): [SparkRowDecoder] builds the column-index map on first access by
 *   scanning `row.schema()`.
 * - **Batch** (preferred for large datasets): a pre-built [IntArray] produced once from the
 *   DataFrame schema is passed to every [deserialize] call, avoiding repeated schema scans.
 *
 * Instances are obtained through [SerializationCache.getSparkDeserializer].
 */
internal class SparkDeserializer<T>(
    private val serializer: KSerializer<T>
) {
    /**
     * Decodes [row] to [T] using [SparkRowDecoder].
     *
     * @param preBuiltColumnIndexMap Pre-built descriptor-index → column-index map. When provided,
     *   [SparkRowDecoder] skips its own schema scan, reducing per-row overhead in batch decoding.
     *   Construct this array once via `schema.fieldIndex()` over the DataFrame schema.
     */
    fun deserialize(row: Row, preBuiltColumnIndexMap: IntArray? = null): T {
        val decoder = SparkRowDecoder(row, preBuiltColumnIndexMap = preBuiltColumnIndexMap)
        return decoder.decodeSerializableValue(serializer)
    }
}
