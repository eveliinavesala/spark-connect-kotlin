package serialization.decoders

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row

/**
 * Original position-based decoder — reads `row.get(descriptorIndex)` without any name lookup.
 *
 * This implementation produces correct results when the DataFrame column order matches the
 * Kotlin descriptor field order exactly. It produces incorrect results — or throws — when
 * an external source (Parquet, Delta, Iceberg, CSV, catalog DDL) controls the column order
 * independently of the descriptor.
 *
 * Kept in the demo source set as evidence. The production decoder in the library uses
 * name-based resolution via `schema.fieldIndex(elementName)`.
 */
@OptIn(ExperimentalSerializationApi::class)
class PositionalSparkRowDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule = EmptySerializersModule()
) : AbstractDecoder() {

    private var currentIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return if (currentIndex < descriptor.elementsCount) {
            currentIndex++
        } else {
            CompositeDecoder.DECODE_DONE
        }
    }

    // All primitive reads use currentIndex - 1 (positional, NOT name-based).
    // When the DataFrame column at that position does not match the descriptor field,
    // Scala's unboxer throws ClassCastException or an incorrect value is produced.
    override fun decodeBoolean(): Boolean = row.getBoolean(currentIndex - 1)
    override fun decodeByte(): Byte      = row.getByte(currentIndex - 1)
    override fun decodeShort(): Short    = row.getShort(currentIndex - 1)
    override fun decodeInt(): Int        = row.getInt(currentIndex - 1)
    override fun decodeLong(): Long      = row.getLong(currentIndex - 1)
    override fun decodeFloat(): Float    = row.getFloat(currentIndex - 1)
    override fun decodeDouble(): Double  = row.getDouble(currentIndex - 1)
    override fun decodeChar(): Char      = row.getString(currentIndex - 1).first()
    override fun decodeString(): String  = row.getString(currentIndex - 1)

    override fun decodeNotNullMark(): Boolean = !row.isNullAt(currentIndex - 1)
    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = row.getString(currentIndex - 1)
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.CLASS -> this
            else -> error("PositionalSparkRowDecoder: complex structure kind ${descriptor.kind} not supported in evidence tests")
        }
    }

    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T =
        super.decodeSerializableValue(deserializer)
}
