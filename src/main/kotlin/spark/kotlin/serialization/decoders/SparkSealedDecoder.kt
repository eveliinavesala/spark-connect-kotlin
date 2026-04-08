package spark.kotlin.serialization.decoders

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractDecoder
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row

/**
 * Decoder for sealed classes (polymorphic types).
 *
 * Reads the _type discriminator field to determine the actual
 * subclass type, then delegates to appropriate decoder.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedDecoder(
    private val row: Row,
    override val serializersModule: SerializersModule
) : AbstractDecoder() {

    private var elementIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        return when {
            elementIndex < descriptor.elementsCount -> elementIndex++
            else -> CompositeDecoder.DECODE_DONE
        }
    }

    override fun decodeString(): String {
        // First element (index 0) is the type discriminator
        return row.getString(elementIndex - 1)
    }

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ): T {
        // After reading the discriminator at index 0, we decode the actual value at index 1
        // The actual value uses the entire row (including the discriminator field)
        val valueDecoder = SparkRowDecoder(row, serializersModule)
        return valueDecoder.decodeSerializableValue(deserializer)
    }
}
