package spark.kotlin.serialization.decoders

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row

/**
 * Decoders for collection types (List and Map).
 *
 * These decoders handle the deserialization of Spark's Scala-based
 * collections back to Kotlin collections.
 */

// ============================================================================
// List Decoder
// ============================================================================

/**
 * Decoder for List/Array types from Spark.
 * Reads elements from a Spark list field.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkListDecoder(
    private val list: List<Any?>,
    override val serializersModule: SerializersModule
) : AbstractDecoder() {

    private var currentIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return if (currentIndex < list.size) {
            currentIndex++
        } else {
            CompositeDecoder.DECODE_DONE
        }
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = list.size

    private fun getValue(): Any? = list[currentIndex - 1]

    // Element decoding
    override fun decodeBoolean(): Boolean = getValue() as Boolean
    override fun decodeByte(): Byte = getValue() as Byte
    override fun decodeShort(): Short = getValue() as Short
    override fun decodeInt(): Int = getValue() as Int
    override fun decodeLong(): Long = getValue() as Long
    override fun decodeFloat(): Float = getValue() as Float
    override fun decodeDouble(): Double = getValue() as Double
    override fun decodeChar(): Char = (getValue() as String).first()
    override fun decodeString(): String = getValue() as String

    override fun decodeNotNullMark(): Boolean = getValue() != null
    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = getValue() as String
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.CLASS -> SparkRowDecoder(getValue() as Row, serializersModule)
            PolymorphicKind.SEALED -> SparkSealedDecoder(getValue() as Row, serializersModule)
            else -> this
        }
    }
}

// ============================================================================
// Map Decoder
// ============================================================================

/**
 * Decoder for Map types from Spark.
 * Reads key-value pairs from a Spark map field.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkMapDecoder(
    private val map: Map<Any?, Any?>,
    override val serializersModule: SerializersModule
) : AbstractDecoder() {

    private val entries = map.entries.toList()
    private var currentIndex = 0
    private var isKey = true

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return if (currentIndex < entries.size * 2) {
            currentIndex++
        } else {
            CompositeDecoder.DECODE_DONE
        }
    }

    override fun decodeCollectionSize(descriptor: SerialDescriptor): Int = map.size

    private fun getValue(): Any? {
        val entryIndex = (currentIndex - 1) / 2
        return if (isKey) {
            entries[entryIndex].key.also { isKey = false }
        } else {
            entries[entryIndex].value.also { isKey = true }
        }
    }

    // Element decoding - alternates between keys and values
    override fun decodeBoolean(): Boolean = getValue() as Boolean
    override fun decodeByte(): Byte = getValue() as Byte
    override fun decodeShort(): Short = getValue() as Short
    override fun decodeInt(): Int = getValue() as Int
    override fun decodeLong(): Long = getValue() as Long
    override fun decodeFloat(): Float = getValue() as Float
    override fun decodeDouble(): Double = getValue() as Double
    override fun decodeChar(): Char = (getValue() as String).first()
    override fun decodeString(): String = getValue() as String

    override fun decodeNotNullMark(): Boolean = getValue() != null
    override fun decodeNull(): Nothing? = null
}
