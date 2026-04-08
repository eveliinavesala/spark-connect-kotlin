package spark.kotlin.serialization.encoders

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Encoder for struct/object fields within a parent structure.
 *
 * Collects field values and passes them to the parent encoder
 * when structure encoding is complete.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkStructEncoder(
    private val parent: SparkRowEncoder
) : AbstractEncoder() {
    private val fieldValues = mutableListOf<Any?>()

    override val serializersModule: SerializersModule
        get() = parent.serializersModule

    // Primitive encoding - add to field values
    override fun encodeBoolean(value: Boolean) { fieldValues.add(value) }
    override fun encodeByte(value: Byte) { fieldValues.add(value) }
    override fun encodeShort(value: Short) { fieldValues.add(value) }
    override fun encodeInt(value: Int) { fieldValues.add(value) }
    override fun encodeLong(value: Long) { fieldValues.add(value) }
    override fun encodeFloat(value: Float) { fieldValues.add(value) }
    override fun encodeDouble(value: Double) { fieldValues.add(value) }
    override fun encodeChar(value: Char) { fieldValues.add(value.toString()) }
    override fun encodeString(value: String) { fieldValues.add(value) }
    override fun encodeNull() { fieldValues.add(null) }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        fieldValues.add(enumDescriptor.getElementName(index))
    }

    // Nested structures
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> SparkListEncoder(this)
            StructureKind.MAP -> SparkMapEncoder(this)
            else -> SparkNestedStructEncoder(this)
        }
    }

    // Handle special types
    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        when (serializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val localDate = value as LocalDate
                fieldValues.add(Date.valueOf(localDate.toJavaLocalDate().toString()))
            }
            "kotlinx.datetime.Instant" -> {
                val instant = value as Instant
                fieldValues.add(Timestamp.from(instant.toJavaInstant()))
            }
            else -> super.encodeSerializableValue(serializer, value)
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // Convert fieldValues to a GenericRow instead of adding the raw list
        val row = GenericRow(fieldValues.toTypedArray())
        parent.addValue(row)
    }

    /**
     * Internal method to add a value to this struct.
     * Used by nested encoders.
     */
    internal fun addValue(value: Any?) {
        fieldValues.add(value)
    }
}

/**
 * Encoder for deeply nested structures.
 * Similar to SparkStructEncoder but works with SparkStructEncoder as parent.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkNestedStructEncoder(
    private val parent: SparkStructEncoder
) : AbstractEncoder() {
    private val fieldValues = mutableListOf<Any?>()

    override val serializersModule: SerializersModule
        get() = parent.serializersModule

    override fun encodeBoolean(value: Boolean) { fieldValues.add(value) }
    override fun encodeByte(value: Byte) { fieldValues.add(value) }
    override fun encodeShort(value: Short) { fieldValues.add(value) }
    override fun encodeInt(value: Int) { fieldValues.add(value) }
    override fun encodeLong(value: Long) { fieldValues.add(value) }
    override fun encodeFloat(value: Float) { fieldValues.add(value) }
    override fun encodeDouble(value: Double) { fieldValues.add(value) }
    override fun encodeChar(value: Char) { fieldValues.add(value.toString()) }
    override fun encodeString(value: String) { fieldValues.add(value) }
    override fun encodeNull() { fieldValues.add(null) }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        fieldValues.add(enumDescriptor.getElementName(index))
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // Convert fieldValues to a GenericRow instead of adding the raw list
        val row = GenericRow(fieldValues.toTypedArray())
        parent.addValue(row)
    }
}
