package spark.kotlin.serialization

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.*
import org.apache.spark.sql.types.*

/**
 * Schema inference from kotlinx.serialization descriptors to Spark types.
 *
 * This file handles the conversion of Kotlin type information (via SerialDescriptor)
 * into Spark SQL schema (StructType, DataType, etc.).
 */

// ============================================================================
// Schema Inference
// ============================================================================

/**
 * Infer a Spark StructType schema from a SerialDescriptor.
 *
 * @param descriptor The kotlinx.serialization descriptor describing the type
 * @return A Spark StructType that can represent the Kotlin type
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
 * Infer a Spark DataType from a SerialDescriptor.
 *
 * @param descriptor The kotlinx.serialization descriptor describing the type
 * @param isNullable Whether the field can be null (unused currently but kept for future use)
 * @return The corresponding Spark DataType
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
