package spark.kotlin.serialization

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.*
import org.apache.spark.sql.types.*

/**
 * Schema inference from kotlinx.serialization [SerialDescriptor]s to Spark SQL types.
 *
 * Provides [inferSparkSchema] and [inferSparkType] — the descriptor-driven counterparts to the
 * reflection-backend functions in `reflect/SchemaInference.kt`. Schema derivation is purely
 * structural and requires no runtime instances; it operates entirely on descriptor metadata.
 */

// ============================================================================
// Schema Inference
// ============================================================================

/**
 * Derives a Spark [StructType] from a [SerialDescriptor].
 *
 * For `CLASS` descriptors each element becomes a [org.apache.spark.sql.types.StructField];
 * nullability is taken from the element descriptor's [SerialDescriptor.isNullable] flag, and
 * field ordering matches the declaration order in the [kotlinx.serialization.Serializable] class.
 *
 * For `SEALED` descriptors a flat union schema is produced:
 * - Column 0: `_type` ([org.apache.spark.sql.types.StringType], non-nullable) — the subtype discriminator
 * - Columns 1..N: one nullable column per unique field name across all subtype `CLASS` descriptors,
 *   in first-seen order. Fields shared by multiple subtypes appear only once.
 *
 * Results are not cached here; caching is the responsibility of [SerializationCache].
 */
@OptIn(ExperimentalSerializationApi::class)
internal fun inferSparkSchema(descriptor: SerialDescriptor): StructType {
    // Sealed classes use a flat union schema: _type discriminator + all unique subtype fields (nullable)
    if (descriptor.kind == PolymorphicKind.SEALED) {
        val fields = mutableListOf<StructField>()
        fields.add(DataTypes.createStructField("_type", DataTypes.StringType, false))
        val seen = linkedMapOf<String, DataType>()
        // element[1] is the CONTEXTUAL "value" container whose elements are the subtypes
        val valueDescriptor = descriptor.getElementDescriptor(1)
        for (i in 0 until valueDescriptor.elementsCount) {
            val subtypeClassDescriptor = valueDescriptor.getElementDescriptor(i)
            for (j in 0 until subtypeClassDescriptor.elementsCount) {
                val name = subtypeClassDescriptor.getElementName(j)
                if (name !in seen) {
                    seen[name] = inferSparkType(subtypeClassDescriptor.getElementDescriptor(j), false)
                }
            }
        }
        seen.forEach { (name, type) ->
            fields.add(DataTypes.createStructField(name, type, true))
        }
        return DataTypes.createStructType(fields.toTypedArray())
    }

    val fields = mutableListOf<StructField>()
    for (i in 0 until descriptor.elementsCount) {
        val fieldName = descriptor.getElementName(i)
        val fieldDescriptor = descriptor.getElementDescriptor(i)
        val isNullable = fieldDescriptor.isNullable
        val dataType = inferSparkType(fieldDescriptor, isNullable)
        fields.add(DataTypes.createStructField(fieldName, dataType, isNullable))
    }
    return DataTypes.createStructType(fields.toTypedArray())
}

/**
 * Maps a single [SerialDescriptor] to the corresponding Spark [DataType].
 *
 * - Primitive kinds → scalar Spark types. `STRING` descriptors with serial name
 *   `kotlinx.datetime.LocalDate` or `kotlinx.datetime.Instant` map to [org.apache.spark.sql.types.DateType]
 *   and [org.apache.spark.sql.types.TimestampType] respectively; all others map to [org.apache.spark.sql.types.StringType].
 * - `ENUM` → [org.apache.spark.sql.types.StringType] (stored as the constant name).
 * - `LIST` → [org.apache.spark.sql.types.ArrayType] with element type inferred recursively; elements are nullable.
 * - `MAP` → [org.apache.spark.sql.types.MapType] with key and value types inferred recursively; values are nullable.
 * - `CLASS` → [org.apache.spark.sql.types.StructType] via [inferSparkSchema], except for datetime special cases above.
 * - `SEALED` → [org.apache.spark.sql.types.StructType] via [inferSparkSchema] (flat union schema).
 * - Unknown kinds → [org.apache.spark.sql.types.StringType] as a fallback.
 *
 * @param isNullable Reserved for future use; not currently consulted during type mapping.
 */
@OptIn(ExperimentalSerializationApi::class)
internal fun inferSparkType(descriptor: SerialDescriptor, isNullable: Boolean): DataType {
    return when (descriptor.kind) {
        // Primitive types
        PrimitiveKind.BOOLEAN -> DataTypes.BooleanType
        PrimitiveKind.BYTE -> DataTypes.ByteType
        PrimitiveKind.SHORT -> DataTypes.ShortType
        PrimitiveKind.INT -> DataTypes.IntegerType
        PrimitiveKind.LONG -> DataTypes.LongType
        PrimitiveKind.FLOAT -> DataTypes.FloatType
        PrimitiveKind.DOUBLE -> DataTypes.DoubleType
        PrimitiveKind.STRING -> when (descriptor.serialName) {
            "kotlinx.datetime.LocalDate" -> DataTypes.DateType
            "kotlinx.datetime.Instant" -> DataTypes.TimestampType
            else -> DataTypes.StringType
        }
        PrimitiveKind.CHAR -> DataTypes.StringType

        // Enum
        SerialKind.ENUM -> DataTypes.StringType

        // Collections
        StructureKind.LIST -> {
            val elementDescriptor = descriptor.getElementDescriptor(0)
            val elementType = inferSparkType(elementDescriptor, false)
            DataTypes.createArrayType(elementType, true)
        }

        StructureKind.MAP -> {
            val keyDescriptor = descriptor.getElementDescriptor(0)
            val valueDescriptor = descriptor.getElementDescriptor(1)
            val keyType = inferSparkType(keyDescriptor, false)
            val valueType = inferSparkType(valueDescriptor, false)
            DataTypes.createMapType(keyType, valueType, true)
        }

        // Complex types
        StructureKind.CLASS -> {
            // Handle special types
            when (descriptor.serialName) {
                "kotlinx.datetime.LocalDate" -> DataTypes.DateType
                "kotlinx.datetime.Instant" -> DataTypes.TimestampType
                else -> inferSparkSchema(descriptor)
            }
        }

        PolymorphicKind.SEALED -> inferSparkSchema(descriptor)

        else -> DataTypes.StringType // Fallback
    }
}
