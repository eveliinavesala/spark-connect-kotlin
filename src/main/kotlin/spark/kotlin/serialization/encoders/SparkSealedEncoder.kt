package spark.kotlin.serialization.encoders

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

/**
 * [AbstractEncoder] for sealed class (`SEALED`) descriptors.
 *
 * Produces a flat [GenericRowWithSchema] containing:
 * - Position 0: `_type` string — the subtype simple name captured from the first [encodeString] call.
 * - Positions 1..N: field values for the active subtype placed at their schema-position indices;
 *   positions belonging to other subtypes remain `null`.
 *
 * Encoding proceeds in two phases:
 * 1. kotlinx.serialization calls [encodeString] with the discriminator, then [beginStructure]
 *    with the subtype descriptor — delegation proceeds to [SparkSealedSubtypeEncoder] which
 *    records each field into [capturedFields] keyed by field name.
 * 2. On [endStructure], a `values` array of size `sealedSchema.fields().size` is allocated,
 *    [typeName] is placed at index 0, and captured field values are placed at their schema
 *    positions by name lookup.
 *
 * [sealedSchema] must be the flat union schema produced by [spark.kotlin.serialization.inferSparkSchema]
 * for the sealed descriptor. When used as a list element, [addToParent] receives only the fields
 * (not the full row) to be appended to the parent list.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedEncoder(
    private val addToParent: (Any?) -> Unit,
    override val serializersModule: SerializersModule,
    private val sealedSchema: StructType,
) : AbstractEncoder() {
    private var typeName: String? = null
    private val capturedFields = mutableMapOf<String, Any?>()

    override fun encodeString(value: String) {
        if (typeName == null) typeName = value // first string is the type discriminator
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder =
        SparkSealedSubtypeEncoder(capturedFields, serializersModule)

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
 * [AbstractEncoder] that captures a sealed subtype's field values by name.
 *
 * [encodeElement] records the current field name from the descriptor before each primitive
 * encode call; the primitive encode then stores the value under that name in [capturedFields].
 * [SparkSealedEncoder] reads [capturedFields] on [endStructure] to place values at their
 * correct positions in the flat union row.
 *
 * Nested structures within sealed subtype fields are not currently supported;
 * [beginStructure] returns `this` as a no-op.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedSubtypeEncoder(
    private val capturedFields: MutableMap<String, Any?>,
    override val serializersModule: SerializersModule,
) : AbstractEncoder() {
    private var currentFieldName: String = ""

    override fun encodeElement(
        descriptor: SerialDescriptor,
        index: Int,
    ): Boolean {
        currentFieldName = descriptor.getElementName(index)
        return true
    }

    override fun encodeBoolean(value: Boolean) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeByte(value: Byte) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeShort(value: Short) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeInt(value: Int) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeLong(value: Long) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeFloat(value: Float) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeDouble(value: Double) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeChar(value: Char) {
        capturedFields[currentFieldName] = value.toString()
    }

    override fun encodeString(value: String) {
        capturedFields[currentFieldName] = value
    }

    override fun encodeNull() {
        capturedFields[currentFieldName] = null
    }

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        capturedFields[currentFieldName] = enumDescriptor.getElementName(index)
    }

    // Nested structures within sealed subtype fields are not yet supported
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder = this

    override fun endStructure(descriptor: SerialDescriptor) = Unit
}
