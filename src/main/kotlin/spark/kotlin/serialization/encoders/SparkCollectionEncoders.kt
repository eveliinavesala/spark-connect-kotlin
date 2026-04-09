package spark.kotlin.serialization.encoders

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.StructureKind
import spark.kotlin.serialization.inferSparkSchema
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule
import scala.jdk.javaapi.CollectionConverters
import java.sql.Date
import java.sql.Timestamp
import scala.collection.immutable.Map

/**
 * Encoders for collection types (List and Map).
 *
 * These encoders handle the serialization of Kotlin collections
 * to Spark's Scala-based collection types.
 */

// ============================================================================
// List Encoder
// ============================================================================

/**
 * Encoder for List/Array types.
 * Collects elements and converts to Scala Seq for Spark.
 *
 * @param addToParent callback invoked with the finished ScalaSeq when encoding completes
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkListEncoder(
    private val addToParent: (Any?) -> Unit,
    override val serializersModule: SerializersModule
) : AbstractEncoder() {
    private val elements = mutableListOf<Any?>()

    // Element encoding
    override fun encodeBoolean(value: Boolean) { elements.add(value) }
    override fun encodeByte(value: Byte) { elements.add(value) }
    override fun encodeShort(value: Short) { elements.add(value) }
    override fun encodeInt(value: Int) { elements.add(value) }
    override fun encodeLong(value: Long) { elements.add(value) }
    override fun encodeFloat(value: Float) { elements.add(value) }
    override fun encodeDouble(value: Double) { elements.add(value) }
    override fun encodeChar(value: Char) { elements.add(value.toString()) }
    override fun encodeString(value: String) { elements.add(value) }
    override fun encodeNull() { elements.add(null) }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        elements.add(enumDescriptor.getElementName(index))
    }

    // Handle special types
    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        when (serializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val localDate = value as LocalDate
                elements.add(Date.valueOf(localDate.toJavaLocalDate()))
            }
            "kotlinx.datetime.Instant" -> {
                val instant = value as Instant
                elements.add(Timestamp.from(instant.toJavaInstant()))
            }
            else -> super.encodeSerializableValue(serializer, value)
        }
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> SparkListEncoder({ elements.add(it) }, serializersModule)
            StructureKind.MAP  -> SparkMapEncoder({ elements.add(it) }, serializersModule)
            StructureKind.CLASS -> when (descriptor.serialName) {
                "kotlinx.datetime.LocalDate", "kotlinx.datetime.Instant" -> this
                else -> SparkStructEncoder({ elements.add(it) }, serializersModule)
            }
            PolymorphicKind.SEALED -> SparkSealedEncoder({ elements.add(it) }, serializersModule, inferSparkSchema(descriptor))
            else -> this
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        addToParent(CollectionConverters.asScala(elements).toSeq())
    }
}

// ============================================================================
// Map Encoder
// ============================================================================

/**
 * Encoder for Map types.
 * Collects key-value pairs and converts to Scala immutable Map for Spark.
 *
 * @param addToParent callback invoked with the finished Scala Map when encoding completes
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkMapEncoder(
    private val addToParent: (Any?) -> Unit,
    override val serializersModule: SerializersModule
) : AbstractEncoder() {
    private val keys = mutableListOf<Any?>()
    private val values = mutableListOf<Any?>()
    private var isKey = true

    // Element encoding - alternates between keys and values
    override fun encodeBoolean(value: Boolean) = addElement(value)
    override fun encodeByte(value: Byte) = addElement(value)
    override fun encodeShort(value: Short) = addElement(value)
    override fun encodeInt(value: Int) = addElement(value)
    override fun encodeLong(value: Long) = addElement(value)
    override fun encodeFloat(value: Float) = addElement(value)
    override fun encodeDouble(value: Double) = addElement(value)
    override fun encodeChar(value: Char) = addElement(value.toString())
    override fun encodeString(value: String) = addElement(value)
    override fun encodeNull() = addElement(null)

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        addElement(enumDescriptor.getElementName(index))
    }

    private fun addElement(value: Any?) {
        if (isKey) keys.add(value) else values.add(value)
        isKey = !isKey
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return when (descriptor.kind) {
            StructureKind.LIST -> SparkListEncoder({ addElement(it) }, serializersModule)
            StructureKind.MAP  -> SparkMapEncoder({ addElement(it) }, serializersModule)
            StructureKind.CLASS -> when (descriptor.serialName) {
                "kotlinx.datetime.LocalDate", "kotlinx.datetime.Instant" -> this
                else -> SparkStructEncoder({ addElement(it) }, serializersModule)
            }
            else -> this
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        val javaMap = keys.zip(values).toMap()
        addToParent(Map.from(CollectionConverters.asScala(javaMap)))
    }
}
