package spark.kotlin.serialization.decoders

import kotlinx.datetime.LocalDate
import kotlinx.datetime.toKotlinInstant
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * [AbstractDecoder] for a nested struct field within a parent [Row].
 *
 * Reads the inner [Row] from the parent via `row.getStruct(fieldIndex)` and decodes its
 * fields using the same name-based column resolution as [SparkRowDecoder].
 *
 * Schema resolution for column mapping:
 * 1. `struct.schema()` — available when the inner row is a
 *    [org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema].
 * 2. Parent row field type — `row.schema().fields()[fieldIndex].dataType()` cast to
 *    [org.apache.spark.sql.types.StructType], used when the inner row is a plain
 *    [org.apache.spark.sql.catalyst.expressions.GenericRow] produced by
 *    [spark.kotlin.serialization.encoders.SparkStructEncoder].
 * 3. Positional fallback — identity mapping, used when neither schema source is available.
 *
 * Nested lists, maps, and further structs are delegated to [SparkListDecoder],
 * [SparkMapDecoder], and a new [SparkStructDecoder] instance respectively.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkStructDecoder(
    private val row: Row,
    private val fieldIndex: Int,
    override val serializersModule: SerializersModule,
) : AbstractDecoder() {
    private val struct: Row by lazy { row.getStruct(fieldIndex) }

    // Name-resolved column index map — same approach as SparkRowDecoder.
    private var columnIndexMap: IntArray? = null
    private var resolvedColumnIndex = 0
    private var descriptorIndex = 0

    private fun buildColumnMap(descriptor: SerialDescriptor) {
        if (columnIndexMap != null) return
        // struct.schema() is null for GenericRow (e.g. produced by SparkStructEncoder).
        // In that case, derive the nested struct's schema from the parent row's field type.
        // Fall back to positional mapping if no schema is available (maintains round-trip correctness).
        val schema: StructType? =
            struct.schema()
                ?: (
                    row
                        .schema()
                        ?.fields()
                        ?.getOrNull(fieldIndex)
                        ?.dataType() as? StructType
                )
        columnIndexMap =
            if (schema != null) {
                IntArray(descriptor.elementsCount) { i ->
                    try {
                        schema.fieldIndex(descriptor.getElementName(i))
                    } catch (_: IllegalArgumentException) {
                        -1
                    }
                }
            } else {
                IntArray(descriptor.elementsCount) { it } // positional fallback
            }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        buildColumnMap(descriptor)
        while (descriptorIndex < descriptor.elementsCount) {
            val colIdx = columnIndexMap!![descriptorIndex]
            descriptorIndex++
            if (colIdx >= 0) {
                resolvedColumnIndex = colIdx
                return descriptorIndex - 1
            }
        }
        return CompositeDecoder.DECODE_DONE
    }

    // Primitive decoding from nested struct — name-resolved, not positional
    override fun decodeBoolean(): Boolean = struct.getBoolean(resolvedColumnIndex)

    override fun decodeByte(): Byte = struct.getByte(resolvedColumnIndex)

    override fun decodeShort(): Short = struct.getShort(resolvedColumnIndex)

    override fun decodeInt(): Int = struct.getInt(resolvedColumnIndex)

    override fun decodeLong(): Long = struct.getLong(resolvedColumnIndex)

    override fun decodeFloat(): Float = struct.getFloat(resolvedColumnIndex)

    override fun decodeDouble(): Double = struct.getDouble(resolvedColumnIndex)

    override fun decodeChar(): Char = struct.getString(resolvedColumnIndex).first()

    override fun decodeString(): String = struct.getString(resolvedColumnIndex)

    override fun decodeNotNullMark(): Boolean = !struct.isNullAt(resolvedColumnIndex)

    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = struct.getString(resolvedColumnIndex)
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    // Nested structures
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder =
        when (descriptor.kind) {
            StructureKind.LIST -> {
                // getList returns a Scala Seq wrapped as Java List - convert to a proper Kotlin list
                val sparkList = struct.getList<Any>(resolvedColumnIndex)
                val kotlinList = sparkList.toList()
                SparkListDecoder(kotlinList, serializersModule)
            }

            StructureKind.MAP -> {
                val map = struct.getJavaMap<Any, Any>(resolvedColumnIndex)
                SparkMapDecoder(map, serializersModule)
            }

            else -> {
                SparkStructDecoder(struct, resolvedColumnIndex, serializersModule)
            }
        }

    // Handle special types
    @Suppress("UNCHECKED_CAST")
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T =
        when (deserializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val date = struct.getDate(resolvedColumnIndex)
                LocalDate.parse(date.toString()) as T
            }

            "kotlinx.datetime.Instant" -> {
                val timestamp = struct.getTimestamp(resolvedColumnIndex)
                timestamp.toInstant().toKotlinInstant() as T
            }

            else -> {
                super.decodeSerializableValue(deserializer)
            }
        }
}
