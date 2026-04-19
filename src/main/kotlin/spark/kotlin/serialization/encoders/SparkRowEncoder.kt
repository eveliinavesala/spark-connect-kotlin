package spark.kotlin.serialization.encoders

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
 * Root [AbstractEncoder] that converts a Kotlin object to a Spark [Row] via a [kotlinx.serialization.KSerializer].
 *
 * Encoded field values are accumulated in [values] in declaration order. When encoding is
 * complete, [getRow] wraps them in a [GenericRowWithSchema] bound to the provided [schema].
 *
 * Delegation on [beginStructure]:
 * - `LIST` → [SparkListEncoder] (produces a Scala `Seq`)
 * - `MAP` → [SparkMapEncoder] (produces a Scala immutable `Map`)
 * - `CLASS` at depth 1 (root) → this encoder (root fields collected in-place)
 * - `CLASS` at depth > 1 (nested) → [SparkStructEncoder] (produces a
 *   [org.apache.spark.sql.catalyst.expressions.GenericRow])
 * - `SEALED` → [SparkSealedEncoder] (produces a flat [GenericRowWithSchema]; its columns are
 *   unpacked and appended to [values] by the `addToParent` callback)
 * - `kotlinx.datetime.*` → special-cased in [encodeSerializableValue]; `beginStructure` returns this
 *
 * [structureDepth] tracks nesting so that the root object and nested objects follow different paths.
 * Child encoders append their result to [values] via [addValue] as their [endStructure] is called.
 */
@OptIn(ExperimentalSerializationApi::class)
internal class SparkRowEncoder(
    private val schema: StructType,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : AbstractEncoder() {
    private val values = mutableListOf<Any?>()
    private var currentIndex = 0
    private var structureDepth = 0

    /**
     * Get the final Spark Row after encoding is complete.
     */
    fun getRow(): Row = GenericRowWithSchema(values.toTypedArray(), schema)

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

    override fun encodeEnum(
        enumDescriptor: SerialDescriptor,
        index: Int,
    ) {
        values.add(enumDescriptor.getElementName(index))
        currentIndex++
    }

    // Structure encoding - delegate to specialized encoders
    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        structureDepth++
        return when (descriptor.kind) {
            StructureKind.LIST -> {
                SparkListEncoder({ addValue(it) }, serializersModule)
            }

            StructureKind.MAP -> {
                SparkMapEncoder({ addValue(it) }, serializersModule)
            }

            StructureKind.CLASS -> {
                when (descriptor.serialName) {
                    "kotlinx.datetime.LocalDate" -> {
                        this
                    }

                    "kotlinx.datetime.Instant" -> {
                        this
                    }

                    else -> {
                        // Only use SparkStructEncoder for nested structures
                        // Root level (structureDepth == 1) should encode directly
                        if (structureDepth == 1) this else SparkStructEncoder({ addValue(it) }, serializersModule)
                    }
                }
            }

            PolymorphicKind.SEALED -> {
                SparkSealedEncoder(
                    { row ->
                        (row as Row).let { r ->
                            for (i in 0 until r.size()) {
                                addValue(r.get(i))
                            }
                        }
                    },
                    serializersModule,
                    schema,
                )
            }

            else -> {
                this
            }
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        structureDepth--
    }

    // Handle special types (DateTime)
    override fun <T> encodeSerializableValue(
        serializer: SerializationStrategy<T>,
        value: T,
    ) {
        when (serializer.descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> {
                val localDate = value as LocalDate
                values.add(Date.valueOf(localDate.toJavaLocalDate()))
                currentIndex++
            }

            "kotlinx.datetime.Instant" -> {
                val instant = value as Instant
                values.add(Timestamp.from(instant.toJavaInstant()))
                currentIndex++
            }

            else -> {
                super.encodeSerializableValue(serializer, value)
            }
        }
    }

    /**
     * Appends [value] to the flat values list.
     * Invoked by child encoders (list, map, struct, sealed) via their `addToParent` callback
     * when their [endStructure] is called.
     */
    internal fun addValue(value: Any?) {
        values.add(value)
    }
}
