package serialization.encoders

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import java.sql.Date
import java.sql.Timestamp

/**
 * Root encoder that converts Kotlin objects to Spark Rows.
 *
 * This encoder is the entry point for serialization and delegates to
 * specialized encoders for different data structures.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkRowEncoder(
    private val schema: StructType,
    override val serializersModule: SerializersModule = EmptySerializersModule()
) : AbstractEncoder() {

    private val values = mutableListOf<Any?>()
    private var currentIndex = 0
    private var structureDepth = 0

    /**
     * Get the final Spark Row after encoding is complete.
     */
    fun getRow(): Row {
        return GenericRowWithSchema(values.toTypedArray(), schema)
    }

    // Primitive type encoding
    override fun encodeBoolean(value: Boolean) {
        values.add(value)
        currentIndex++
    }

    override fun encodeByte(value: Byte) {
        values.add(value)
        currentIndex++
    }

    override fun encodeShort(value: Short) {
        values.add(value)
        currentIndex++
    }

    override fun encodeInt(value: Int) {
        values.add(value)
        currentIndex++
    }

    override fun encodeLong(value: Long) {
        values.add(value)
        currentIndex++
    }

    override fun encodeFloat(value: Float) {
        values.add(value)
        currentIndex++
    }

    override fun encodeDouble(value: Double) {
        values.add(value)
        currentIndex++
    }

    override fun encodeChar(value: Char) {
        values.add(value.toString())
        currentIndex++
    }

    override fun encodeString(value: String) {
        values.add(value)
        currentIndex++
    }

    override fun encodeNull() {
        values.add(null)
        currentIndex++
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        values.add(enumDescriptor.getElementName(index))
        currentIndex++
    }

    // Structure encoding - delegate to specialized encoders
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        structureDepth++
        return when (descriptor.kind) {
            StructureKind.LIST -> SparkListEncoder(this)
            StructureKind.MAP -> SparkMapEncoder(this)
            StructureKind.CLASS -> {
                when (descriptor.serialName) {
                    "kotlinx.datetime.LocalDate" -> this
                    "kotlinx.datetime.Instant" -> this
                    else -> {
                        // Only use SparkStructEncoder for nested structures
                        // Root level (structureDepth == 1) should encode directly
                        if (structureDepth == 1) this else SparkStructEncoder(this)
                    }
                }
            }
            PolymorphicKind.SEALED -> SparkSealedEncoder(this)
            else -> this
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        structureDepth--
    }

    // Handle special types (DateTime)
    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        when (serializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val localDate = value as LocalDate
                values.add(Date.valueOf(localDate.toJavaLocalDate().toString()))
                currentIndex++
            }
            "kotlinx.datetime.Instant" -> {
                val instant = value as Instant
                values.add(Timestamp.from(instant.toJavaInstant()))
                currentIndex++
            }
            else -> super.encodeSerializableValue(serializer, value)
        }
    }

    /**
     * Internal method to add a value to the row.
     * Used by child encoders via reflection.
     */
    internal fun addValue(value: Any?) {
        values.add(value)
    }
}
