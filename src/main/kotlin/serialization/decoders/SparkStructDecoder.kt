package serialization.decoders

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

/**
 * Decoder for struct/object fields within a Spark Row.
 *
 * Reads nested struct data from a specific field index in the parent row.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkStructDecoder(
    private val row: Row,
    private val fieldIndex: Int,
    override val serializersModule: SerializersModule
) : AbstractDecoder() {

    private var currentIndex = 0
    private val struct: Row by lazy { row.getStruct(fieldIndex) }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return if (currentIndex < descriptor.elementsCount) {
            currentIndex++
        } else {
            CompositeDecoder.DECODE_DONE
        }
    }

    // Primitive decoding from nested struct
    override fun decodeBoolean(): Boolean = struct.getBoolean(currentIndex - 1)
    override fun decodeByte(): Byte = struct.getByte(currentIndex - 1)
    override fun decodeShort(): Short = struct.getShort(currentIndex - 1)
    override fun decodeInt(): Int = struct.getInt(currentIndex - 1)
    override fun decodeLong(): Long = struct.getLong(currentIndex - 1)
    override fun decodeFloat(): Float = struct.getFloat(currentIndex - 1)
    override fun decodeDouble(): Double = struct.getDouble(currentIndex - 1)
    override fun decodeChar(): Char = struct.getString(currentIndex - 1).first()
    override fun decodeString(): String = struct.getString(currentIndex - 1)

    override fun decodeNotNullMark(): Boolean = !struct.isNullAt(currentIndex - 1)
    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = struct.getString(currentIndex - 1)
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    // Nested structures
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                // getList returns a Scala Seq wrapped as Java List - convert to a proper Kotlin list
                val sparkList = struct.getList<Any>(currentIndex - 1)
                val kotlinList = sparkList.toList()
                SparkListDecoder(kotlinList, serializersModule)
            }
            StructureKind.MAP -> {
                val map = struct.getJavaMap<Any, Any>(currentIndex - 1)
                SparkMapDecoder(map, serializersModule)
            }
            else -> SparkStructDecoder(struct, currentIndex - 1, serializersModule)
        }
    }

    // Handle special types
    @Suppress("UNCHECKED_CAST")
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return when (deserializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val date = struct.getDate(currentIndex - 1)
                LocalDate.parse(date.toString()) as T
            }
            "kotlinx.datetime.Instant" -> {
                val timestamp = struct.getTimestamp(currentIndex - 1)
                timestamp.toInstant().toKotlinInstant() as T
            }
            else -> super.decodeSerializableValue(deserializer)
        }
    }
}
