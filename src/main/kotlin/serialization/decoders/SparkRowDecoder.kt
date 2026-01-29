package serialization.decoders

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
 * Root decoder that converts Spark Rows to Kotlin objects.
 *
 * This decoder is the entry point for deserialization and delegates to
 * specialized decoders for different data structures.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkRowDecoder(
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

    // Primitive type decoding
    override fun decodeBoolean(): Boolean = row.getBoolean(currentIndex - 1)
    override fun decodeByte(): Byte = row.getByte(currentIndex - 1)
    override fun decodeShort(): Short = row.getShort(currentIndex - 1)
    override fun decodeInt(): Int = row.getInt(currentIndex - 1)
    override fun decodeLong(): Long = row.getLong(currentIndex - 1)
    override fun decodeFloat(): Float = row.getFloat(currentIndex - 1)
    override fun decodeDouble(): Double = row.getDouble(currentIndex - 1)
    override fun decodeChar(): Char = row.getString(currentIndex - 1).first()
    override fun decodeString(): String = row.getString(currentIndex - 1)

    override fun decodeNotNullMark(): Boolean = !row.isNullAt(currentIndex - 1)
    override fun decodeNull(): Nothing? = null

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        val enumName = row.getString(currentIndex - 1)
        return (0 until enumDescriptor.elementsCount).find {
            enumDescriptor.getElementName(it) == enumName
        } ?: throw SerializationException("Unknown enum value: $enumName")
    }

    // Structure decoding - delegate to specialized decoders
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                // getList returns a Scala Seq wrapped as Java List - convert to a proper Kotlin list
                val sparkList = row.getList<Any>(currentIndex - 1)
                val kotlinList = sparkList.toList()
                SparkListDecoder(kotlinList, serializersModule)
            }
            StructureKind.MAP -> {
                val map = row.getJavaMap<Any, Any>(currentIndex - 1)
                SparkMapDecoder(map, serializersModule)
            }
            StructureKind.CLASS -> {
                when (descriptor.serialName) {
                    "kotlinx.datetime.LocalDate",
                    "kotlinx.datetime.Instant" -> this
                    else -> {
                        // If we're at the root level (currentIndex == 0), decode in place
                        // Otherwise, create a new decoder for the nested struct
                        if (currentIndex == 0) {
                            this
                        } else {
                            SparkStructDecoder(row, currentIndex - 1, serializersModule)
                        }
                    }
                }
            }
            PolymorphicKind.SEALED -> SparkSealedDecoder(row, serializersModule)
            else -> this
        }
    }

    // Handle special types (DateTime)
    @Suppress("UNCHECKED_CAST")
    override fun <T> decodeSerializableValue(deserializer: DeserializationStrategy<T>): T {
        return when (deserializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val date = row.getDate(currentIndex - 1)
                LocalDate.parse(date.toString()) as T
            }
            "kotlinx.datetime.Instant" -> {
                val timestamp = row.getTimestamp(currentIndex - 1)
                timestamp.toInstant().toKotlinInstant() as T
            }
            else -> super.decodeSerializableValue(deserializer)
        }
    }
}
