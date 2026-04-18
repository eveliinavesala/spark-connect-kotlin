package spark.kotlin.serialization.decoders

import kotlinx.datetime.LocalDate
import kotlinx.datetime.toKotlinInstant
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row

/**
 * Root [AbstractDecoder] that maps a Spark [Row] to a Kotlin object via a [kotlinx.serialization.KSerializer].
 *
 * Column resolution is name-based rather than positional: [decodeElementIndex] builds an
 * `IntArray` that maps each descriptor element index to its column index in the [Row] by looking
 * up the element name in `row.schema()`. This makes decoding robust to column reordering between
 * the descriptor's declaration order and the DataFrame schema. Columns absent in the schema
 * receive index `-1` and are skipped (treated as null for nullable fields).
 *
 * Two schema-scan paths are provided:
 * - **Per-row** (default): [buildColumnMap] is called on the first [decodeElementIndex] invocation
 *   for this instance, scanning `row.schema()` once per row.
 * - **Batch** (preferred): a pre-built [IntArray] is injected via [preBuiltColumnIndexMap],
 *   produced once per batch from the DataFrame schema by [SparkDeserializer]; no per-row scan occurs.
 *
 * Delegation on [beginStructure]:
 * - `LIST` → [SparkListDecoder]
 * - `MAP` → [SparkMapDecoder]
 * - `CLASS` at depth > 0 → [SparkStructDecoder] (reads the nested [Row] at [resolvedColumnIndex])
 * - `CLASS` at depth 0 (root call) → this decoder (fields decoded in-place)
 * - `SEALED` → [SparkSealedDecoder]
 * - `kotlinx.datetime.*` types → special-cased in [decodeSerializableValue]; [beginStructure] returns this
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkRowDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
    // Pre-built descriptor-index → column-index map. When supplied (batch decode path),
    // buildColumnMap is skipped so the schema scan happens once per batch instead of once per row.
    preBuiltColumnIndexMap: IntArray? = null
) : AbstractDecoder() {

    // Maps descriptor element index → actual column index in the Row.
    // Initialized from preBuiltColumnIndexMap if provided; otherwise built lazily on first
    // decodeElementIndex call. A -1 entry means the column is absent in the Row schema.
    private var columnIndexMap: IntArray? = preBuiltColumnIndexMap

    // The column index that the most recent decodeElementIndex resolved to.
    // Primitive decode calls use this value rather than a sequential counter so that
    // column order in the Row schema doesn't need to match descriptor field order.
    private var resolvedColumnIndex = 0
    private var descriptorIndex = 0

    private fun buildColumnMap(descriptor: SerialDescriptor) {
        if (columnIndexMap != null) return
        val schema = row.schema()
        columnIndexMap = IntArray(descriptor.elementsCount) { i ->
            try { schema.fieldIndex(descriptor.getElementName(i)) } catch (_: IllegalArgumentException) { -1 }
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        buildColumnMap(descriptor)
        while (descriptorIndex < descriptor.elementsCount) {
            val colIdx = columnIndexMap!![descriptorIndex]
            descriptorIndex++
            if (colIdx >= 0) {
                resolvedColumnIndex = colIdx
                return descriptorIndex - 1   // descriptor element index (what kotlinx.serialization expects)
            }
            // column absent in this Row — skip (treat as missing/null via decodeNotNullMark)
        }
        return CompositeDecoder.DECODE_DONE
    }

    // Primitive type decoding — all use resolvedColumnIndex (name-resolved, not positional).
    //
    // decodeDouble / decodeFloat / decodeInt / decodeLong use row.get() + explicit cast rather
    // than row.getDouble() / row.getFloat() / etc., because Spark SQL infers numeric literals
    // (e.g. 19.99, 1000) as DecimalType, and Scala's unboxer throws ClassCastException when
    // asked to unbox a java.math.BigDecimal as a primitive Double/Float/Int/Long.
    // row.get() returns the raw JVM value; casting via Number.toXxx() handles all numeric types.
    override fun decodeBoolean(): Boolean = row.getBoolean(resolvedColumnIndex)
    override fun decodeByte(): Byte = row.getByte(resolvedColumnIndex)
    override fun decodeShort(): Short = row.getShort(resolvedColumnIndex)
    override fun decodeInt(): Int = (row.get(resolvedColumnIndex) as? Number)?.toInt() ?: row.getInt(resolvedColumnIndex)
    override fun decodeLong(): Long = (row.get(resolvedColumnIndex) as? Number)?.toLong() ?: row.getLong(resolvedColumnIndex)
    override fun decodeFloat(): Float = (row.get(resolvedColumnIndex) as? Number)?.toFloat() ?: row.getFloat(resolvedColumnIndex)
    override fun decodeDouble(): Double = (row.get(resolvedColumnIndex) as? Number)?.toDouble() ?: row.getDouble(resolvedColumnIndex)
    override fun decodeChar(): Char = row.getString(resolvedColumnIndex).first()
    override fun decodeString(): String = row.getString(resolvedColumnIndex)

    override fun decodeNotNullMark(): Boolean = !row.isNullAt(resolvedColumnIndex)
    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = row.getString(resolvedColumnIndex)
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    // Structure decoding - delegate to specialized decoders
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                // getList returns a Scala Seq wrapped as Java List - convert to a proper Kotlin list
                val sparkList = row.getList<Any>(resolvedColumnIndex)
                val kotlinList = sparkList.toList()
                SparkListDecoder(kotlinList, serializersModule)
            }
            StructureKind.MAP -> {
                val map = row.getJavaMap<Any, Any>(resolvedColumnIndex)
                SparkMapDecoder(map, serializersModule)
            }
            StructureKind.CLASS -> {
                when (descriptor.serialName) {
                    "kotlinx.datetime.LocalDate",
                    "kotlinx.datetime.Instant" -> this
                    else -> {
                        // descriptorIndex == 0 means beginStructure was called for the root class
                        // before any element has been visited — decode in place.
                        if (descriptorIndex == 0) {
                            this
                        } else {
                            SparkStructDecoder(row, resolvedColumnIndex, serializersModule)
                        }
                    }
                }
            }
            PolymorphicKind.SEALED -> SparkSealedDecoder(row, serializersModule)
            else -> this
        }
    }

    // Date and time type decoding
    @Suppress("UNCHECKED_CAST")
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return when (deserializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val date = row.getDate(resolvedColumnIndex)
                LocalDate.parse(date.toString()) as T
            }
            "kotlinx.datetime.Instant" -> {
                val timestamp = row.getTimestamp(resolvedColumnIndex)
                timestamp.toInstant().toKotlinInstant() as T
            }
            else -> super.decodeSerializableValue(deserializer)
        }
    }
}
