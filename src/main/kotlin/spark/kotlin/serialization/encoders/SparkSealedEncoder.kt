package spark.kotlin.serialization.encoders

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
 * Encoder for sealed classes (polymorphic types).
 *
 * Produces a flat Row: [_type discriminator, field1, field2, ...] where all
 * fields from all subtypes appear in the schema and absent fields are null.
 * The schema must be the flat union schema produced by inferSparkSchema for
 * the sealed descriptor.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedEncoder(
    private val addToParent: (Any?) -> Unit,
    override val serializersModule: SerializersModule,
    private val sealedSchema: StructType
) : AbstractEncoder() {

    private var typeName: String? = null
    private val capturedFields = mutableMapOf<String, Any?>()

    override fun encodeString(value: String) {
        if (typeName == null) typeName = value  // first string is the type discriminator
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return SparkSealedSubtypeEncoder(descriptor, capturedFields, serializersModule)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // Build a flat values array matching the sealed schema.
        // Position 0 = _type, positions 1..N = union fields (null if absent in this subtype).
        val values = arrayOfNulls<Any?>(sealedSchema.fields().size)
        values[0] = typeName
        sealedSchema.fields().forEachIndexed { i, field ->
            if (i > 0 && field.name() in capturedFields) {
                values[i] = capturedFields[field.name()]
            }
        }
        addToParent(GenericRowWithSchema(values, sealedSchema))
    }
}

/**
 * Capture encoder for a sealed subtype.
 *
 * Records each field's name→value into [capturedFields] so that
 * [SparkSealedEncoder] can place them at the correct position in the flat Row.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedSubtypeEncoder(
    private val subtypeDescriptor: SerialDescriptor,
    private val capturedFields: MutableMap<String, Any?>,
    override val serializersModule: SerializersModule
) : AbstractEncoder() {

    private var currentFieldName: String = ""

    override fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean {
        currentFieldName = descriptor.getElementName(index)
        return true
    }

    override fun encodeBoolean(value: Boolean) { capturedFields[currentFieldName] = value }
    override fun encodeByte(value: Byte)       { capturedFields[currentFieldName] = value }
    override fun encodeShort(value: Short)     { capturedFields[currentFieldName] = value }
    override fun encodeInt(value: Int)         { capturedFields[currentFieldName] = value }
    override fun encodeLong(value: Long)       { capturedFields[currentFieldName] = value }
    override fun encodeFloat(value: Float)     { capturedFields[currentFieldName] = value }
    override fun encodeDouble(value: Double)   { capturedFields[currentFieldName] = value }
    override fun encodeChar(value: Char)       { capturedFields[currentFieldName] = value.toString() }
    override fun encodeString(value: String)   { capturedFields[currentFieldName] = value }
    override fun encodeNull()                  { capturedFields[currentFieldName] = null }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        capturedFields[currentFieldName] = enumDescriptor.getElementName(index)
    }

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        super.encodeSerializableValue(serializer, value)
    }

    // Nested structures within sealed subtype fields are not yet supported
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder = this
    override fun endStructure(descriptor: SerialDescriptor) {}
}

