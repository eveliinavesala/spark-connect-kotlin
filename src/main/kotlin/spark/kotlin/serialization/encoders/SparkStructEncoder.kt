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
 * [AbstractEncoder] for a nested data class field within a parent encoder.
 *
 * Accumulates field values in [fieldValues] and, on [endStructure], wraps them in a
 * [GenericRow] passed to [addToParent]. The absence of a schema in [GenericRow] (vs
 * [GenericRowWithSchema]) is intentional: the schema is carried by the outermost row,
 * and nested struct schemas are recovered from the parent field type during decoding.
 *
 * @param addToParent Callback invoked with the finished [GenericRow] when [endStructure] is called.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkStructEncoder(
    private val addToParent: (Any?) -> Unit,
    override val serializersModule: SerializersModule
) : AbstractEncoder() {
    private val fieldValues = mutableListOf<Any?>()

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
            StructureKind.LIST -> SparkListEncoder({ fieldValues.add(it) }, serializersModule)
            StructureKind.MAP  -> SparkMapEncoder({ fieldValues.add(it) }, serializersModule)
            StructureKind.CLASS -> when (descriptor.serialName) {
                "kotlinx.datetime.LocalDate", "kotlinx.datetime.Instant" -> this
                else -> SparkStructEncoder({ fieldValues.add(it) }, serializersModule)
            }
            else -> this
        }
    }

    // Handle special types
    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        when (serializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val localDate = value as LocalDate
                fieldValues.add(Date.valueOf(localDate.toJavaLocalDate()))
            }
            "kotlinx.datetime.Instant" -> {
                val instant = value as Instant
                fieldValues.add(Timestamp.from(instant.toJavaInstant()))
            }
            else -> super.encodeSerializableValue(serializer, value)
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        addToParent(GenericRow(fieldValues.toTypedArray()))
    }
}
