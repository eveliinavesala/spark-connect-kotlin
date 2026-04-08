package serialization.encoders

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule

/**
 * Encoder for sealed classes (polymorphic types).
 *
 * Adds a _type discriminator field to distinguish between different
 * subclasses of a sealed class hierarchy.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkSealedEncoder(
    private val parent: SparkRowEncoder
) : AbstractEncoder() {
    private var typeName: String? = null
    private val fieldValues = mutableListOf<Any?>()

    override val serializersModule: SerializersModule
        get() = parent.serializersModule

    override fun encodeString(value: String) {
        if (typeName == null) {
            // First string is the type discriminator
            typeName = value
        } else {
            fieldValues.add(value)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return SparkStructEncoder(parent)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // Add type discriminator first, then all field values
        val allValues = mutableListOf<Any?>(typeName)
        allValues.addAll(fieldValues)
        parent.setValues(allValues)
    }
}

/**
 * Extension function to set values on SparkRowEncoder.
 * Uses reflection to access the private values field.
 */
private fun SparkRowEncoder.setValues(newValues: List<Any?>) {
    val field = this::class.java.getDeclaredField("values")
    field.isAccessible = true
    @Suppress("UNCHECKED_CAST")
    val valuesList = field.get(this) as MutableList<Any?>
    valuesList.clear()
    valuesList.addAll(newValues)
}
