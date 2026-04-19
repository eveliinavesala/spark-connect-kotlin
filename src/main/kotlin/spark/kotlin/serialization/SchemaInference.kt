package spark.kotlin.serialization

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.PolymorphicKind
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.SerialKind
import kotlinx.serialization.descriptors.StructureKind
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

// ============================================================================
// Schema Inference
// ============================================================================

/*
 * Schema inference from kotlinx.serialization [SerialDescriptor]s to Spark SQL types.
 *
 * Provides [inferSparkSchema] and [inferSparkType] — the descriptor-driven counterparts to the
 * reflection-backend functions in `reflect/SchemaInference.kt`. Schema derivation is purely
 * structural and requires no runtime instances; it operates entirely on descriptor metadata.
 */

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
    if (descriptor.kind == PolymorphicKind.SEALED) {
        return inferSealedSchema(descriptor)
    }

    val fields = mutableListOf<StructField>()
    for (i in 0 until descriptor.elementsCount) {
        val fieldName = descriptor.getElementName(i)
        val fieldDescriptor = descriptor.getElementDescriptor(i)
        val isNullable = fieldDescriptor.isNullable
        val dataType = inferSparkType(fieldDescriptor)
        fields.add(DataTypes.createStructField(fieldName, dataType, isNullable))
    }
    return DataTypes.createStructType(fields.toTypedArray())
}

@OptIn(ExperimentalSerializationApi::class)
private fun inferSealedSchema(descriptor: SerialDescriptor): StructType {
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
                seen[name] = inferSparkType(subtypeClassDescriptor.getElementDescriptor(j))
            }
        }
    }

    seen.forEach { (name, type) ->
        fields.add(DataTypes.createStructField(name, type, true))
    }
    return DataTypes.createStructType(fields.toTypedArray())
}

/**
 * Maps a single [SerialDescriptor] to the corresponding Spark [DataType].
 */
@OptIn(ExperimentalSerializationApi::class)
internal fun inferSparkType(descriptor: SerialDescriptor): DataType {
    val serialName = descriptor.serialName.removeSuffix("?")

    return when (val kind = descriptor.kind) {
        is PrimitiveKind -> mapPrimitiveType(kind, serialName)
        is StructureKind -> mapStructureType(kind, descriptor, serialName)
        SerialKind.ENUM -> DataTypes.StringType
        PolymorphicKind.SEALED -> inferSparkSchema(descriptor)
        else -> DataTypes.StringType
    }
}

@OptIn(ExperimentalSerializationApi::class)
private fun mapPrimitiveType(
    kind: PrimitiveKind,
    serialName: String,
): DataType =
    when (kind) {
        PrimitiveKind.BOOLEAN -> {
            DataTypes.BooleanType
        }

        PrimitiveKind.BYTE -> {
            DataTypes.ByteType
        }

        PrimitiveKind.SHORT -> {
            DataTypes.ShortType
        }

        PrimitiveKind.INT -> {
            DataTypes.IntegerType
        }

        PrimitiveKind.LONG -> {
            DataTypes.LongType
        }

        PrimitiveKind.FLOAT -> {
            DataTypes.FloatType
        }

        PrimitiveKind.DOUBLE -> {
            DataTypes.DoubleType
        }

        PrimitiveKind.STRING -> {
            when (serialName) {
                "kotlinx.datetime.LocalDate" -> DataTypes.DateType
                "kotlinx.datetime.Instant" -> DataTypes.TimestampType
                else -> DataTypes.StringType
            }
        }

        PrimitiveKind.CHAR -> {
            DataTypes.StringType
        }
    }

@OptIn(ExperimentalSerializationApi::class)
private fun mapStructureType(
    kind: StructureKind,
    descriptor: SerialDescriptor,
    serialName: String,
): DataType =
    when (kind) {
        StructureKind.LIST -> {
            val elementDescriptor = descriptor.getElementDescriptor(0)
            val elementType = inferSparkType(elementDescriptor)
            DataTypes.createArrayType(elementType, true)
        }

        StructureKind.MAP -> {
            val keyDescriptor = descriptor.getElementDescriptor(0)
            val valueDescriptor = descriptor.getElementDescriptor(1)
            val keyType = inferSparkType(keyDescriptor)
            val valueType = inferSparkType(valueDescriptor)
            DataTypes.createMapType(keyType, valueType, true)
        }

        StructureKind.CLASS -> {
            when (serialName) {
                "kotlinx.datetime.LocalDate" -> DataTypes.DateType
                "kotlinx.datetime.Instant" -> DataTypes.TimestampType
                else -> inferSparkSchema(descriptor)
            }
        }

        else -> {
            DataTypes.StringType
        }
    }
