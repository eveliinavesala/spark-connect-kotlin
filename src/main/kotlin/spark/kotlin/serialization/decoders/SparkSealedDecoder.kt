package spark.kotlin.serialization.decoders

import kotlinx.datetime.LocalDate
import kotlinx.datetime.toKotlinInstant
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row

/**
 * [AbstractDecoder] for sealed class (`SEALED`) descriptors.
 *
 * Reads the `_type` discriminator string from column 0 of the flat union [Row] and exposes it
 * as the first element of the sealed descriptor. When kotlinx.serialization then calls
 * [beginStructure] with the resolved subtype descriptor, delegation proceeds to
 * [SparkSealedSubtypeDecoder] which maps subtype fields by name from the same flat row.
 *
 * The flat union schema expected here is produced by [spark.kotlin.serialization.inferSparkSchema]
 * for `SEALED` descriptors: column 0 is `_type`, followed by one nullable column per unique field
 * name across all subtypes.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule,
) : AbstractDecoder() {
    private var elementIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        when {
            elementIndex < descriptor.elementsCount -> elementIndex++
            else -> CompositeDecoder.DECODE_DONE
        }

    override fun decodeString(): String {
        // _type discriminator is always at column 0
        return row.getString(0)
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        // Called by the subtype serializer; descriptor is the subtype's descriptor
        return SparkSealedSubtypeDecoder(row, descriptor, serializersModule)
    }
}

/**
 * [AbstractDecoder] for a specific sealed subtype within a flat union [Row].
 *
 * Each subtype field is mapped to its flat-row column index by name at construction time;
 * the mapping is stored in [fieldColumnIndices] and reused for every primitive decode call.
 * Fields of other subtypes (absent in this subtype's descriptor) remain in the row as nulls
 * and are simply not visited.
 *
 * Nested structures within sealed subtype fields are not currently supported;
 * [beginStructure] returns `this` as a no-op.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedSubtypeDecoder(
    private val row: Row,
    private val subtypeDescriptor: SerialDescriptor,
    override val serializersModule: SerializersModule,
) : AbstractDecoder() {
    private var currentElementIndex = 0

    // Map subtype field index → flat row column index (by field name lookup)
    private val fieldColumnIndices: IntArray =
        IntArray(subtypeDescriptor.elementsCount) { i ->
            row.fieldIndex(subtypeDescriptor.getElementName(i))
        }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int =
        if (currentElementIndex < descriptor.elementsCount) {
            currentElementIndex++
        } else {
            CompositeDecoder.DECODE_DONE
        }

    private fun col() = fieldColumnIndices[currentElementIndex - 1]

    override fun decodeBoolean(): Boolean = row.getBoolean(col())

    override fun decodeByte(): Byte = row.getByte(col())

    override fun decodeShort(): Short = row.getShort(col())

    override fun decodeInt(): Int = row.getInt(col())

    override fun decodeLong(): Long = row.getLong(col())

    override fun decodeFloat(): Float = row.getFloat(col())

    override fun decodeDouble(): Double = row.getDouble(col())

    override fun decodeChar(): Char = row.getString(col()).first()

    override fun decodeString(): String = row.getString(col())

    override fun decodeNotNullMark(): Boolean = !row.isNullAt(col())

    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = row.getString(col())
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T =
        when (deserializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val date = row.getDate(col())
                LocalDate.parse(date.toString()) as T
            }

            "kotlinx.datetime.Instant" -> {
                val timestamp = row.getTimestamp(col())
                timestamp.toInstant().toKotlinInstant() as T
            }

            else -> {
                super.decodeSerializableValue(deserializer)
            }
        }

    // Nested structures within sealed subtype fields are not yet supported
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder = this

    override fun endStructure(descriptor: SerialDescriptor) = Unit
}
