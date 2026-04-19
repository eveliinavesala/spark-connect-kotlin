package spark.kotlin.reflect

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.math.BigDecimal
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

// Tracks types currently being schema-inferred on each thread to detect recursive definitions.
// Nullability is stripped before lookup so Tree and Tree? are treated as the same type.
private val schemaInProgress = ThreadLocal.withInitial<MutableSet<KType>> { mutableSetOf() }

/**
 * Derives a Spark [StructType] from a Kotlin [KType] using reflection.
 *
 * Supported type families:
 * - Primitives and [String] → scalar Spark types
 * - [kotlinx.datetime.LocalDate] / [kotlinx.datetime.Instant] → [org.apache.spark.sql.types.DateType] / TimestampType
 * - Data classes → nested [StructType] (recursive)
 * - Sealed class hierarchies → flat union schema: one [org.apache.spark.sql.types.StringType] `_type`
 *   discriminator column followed by nullable columns for every field across all leaf subclasses
 * - [Pair] / [Triple] → struct with fields `_1`, `_2` (and `_3`), matching Scala tuple convention
 * - [Collection] / [Array] → [org.apache.spark.sql.types.ArrayType]
 * - [Map] → [org.apache.spark.sql.types.MapType]
 * - Value classes → unwrapped to the backing type's Spark equivalent
 * - [org.apache.spark.sql.types.SQLUserDefinedType]-annotated classes → the registered UDT
 *
 * Recursive type detection is performed via a thread-local set of in-progress types. A type
 * encountered twice in the same inference call results in [IllegalArgumentException].
 *
 * Results are **not** cached here; caching is handled by [ReflectionCache.getSchema].
 *
 * @throws IllegalArgumentException if a recursive type or unsupported type is encountered,
 *   or if a sealed hierarchy contains subclasses with duplicate simple names.
 */
internal fun inferSchemaInternal(kType: KType): StructType {
    val normalizedType = kType.jvmErasure.createType(kType.arguments, nullable = false)
    val inProgress = schemaInProgress.get()
    if (normalizedType in inProgress) {
        throw IllegalArgumentException(
            "Recursive type detected: '${kType.jvmErasure.simpleName}' contains a direct or indirect " +
                "reference to itself. Recursive schemas are not supported — flatten the type or use a UDT.",
        )
    }
    inProgress.add(normalizedType)
    try {
        val kClass = kType.jvmErasure

        if (kClass == Pair::class || kClass == Triple::class) {
            val fields =
                kType.arguments.mapIndexed { index, arg ->
                    val type = arg.type!!
                    StructField("_" + (index + 1), kotlinTypeToSparkType(type), type.isMarkedNullable, Metadata.empty())
                }
            return StructType(fields.toTypedArray())
        }

        val fields =
            if (kClass.isSealed) {
                // Collect all concrete leaf subclasses across the full hierarchy (including intermediate sealed levels)
                val leaves = kClass.allLeafSubclasses()
                // Check for simpleName collisions across the full hierarchy
                val simpleNames = leaves.map { it.simpleName }
                if (simpleNames.distinct().size != simpleNames.size) {
                    val duplicates =
                        simpleNames
                            .groupingBy { it }
                            .eachCount()
                            .filter { it.value > 1 }
                            .keys
                    throw IllegalArgumentException(
                        "Sealed class hierarchy '${kClass.simpleName}' contains subclasses with " +
                            "duplicate simple names: $duplicates. " +
                            "This causes ambiguity in serialization. Please rename subclasses to be unique.",
                    )
                }

                (
                    leaves.flatMap { it.memberProperties }.distinctBy { it.name }.map {
                        StructField(it.name, kotlinTypeToSparkType(it.returnType), true, Metadata.empty())
                    } + StructField("_type", DataTypes.StringType, false, Metadata.empty())
                )
            } else {
                val argMap = buildTypeArgMap(kType)
                kClass.memberProperties.map {
                    val resolvedType = resolveTypeParam(it.returnType, argMap)
                    StructField(
                        it.name,
                        kotlinTypeToSparkType(resolvedType),
                        resolvedType.isMarkedNullable,
                        Metadata.empty(),
                    )
                }
            }
        return StructType(fields.toTypedArray())
    } finally {
        inProgress.remove(normalizedType)
    }
}

/**
 * Maps a single [KType] to the corresponding Spark [DataType].
 *
 * For compound types (data classes, sealed hierarchies, [Pair], [Triple]) the result is
 * a [org.apache.spark.sql.types.StructType] obtained via [ReflectionCache.getSchema] so that
 * sub-schemas are also cached. Value classes are transparently unwrapped to their backing type.
 * [Char] is stored as a single-character [org.apache.spark.sql.types.StringType].
 * [java.math.BigDecimal] maps to `DECIMAL(38, 18)`.
 *
 * @throws IllegalArgumentException if [kType] has no known Spark equivalent and no
 *   [org.apache.spark.sql.types.SQLUserDefinedType] annotation.
 */
private const val DECIMAL_PRECISION = 38
private const val DECIMAL_SCALE = 18

internal fun kotlinTypeToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier.isSubclassOf(Enum::class) -> {
            DataTypes.StringType
        }

        classifier.isSealed || classifier.isData || classifier == Pair::class || classifier == Triple::class -> {
            ReflectionCache.getSchema(kType)
        }

        classifier.isValue -> {
            val underlyingProperty =
                classifier.primaryConstructor!!
                    .parameters
                    .first()
                    .type
            kotlinTypeToSparkType(underlyingProperty)
        }

        else -> {
            kotlinNonStructuralToSparkType(kType)
        }
    }
}

private fun kotlinNonStructuralToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier == LocalDate::class || classifier == java.time.LocalDate::class -> DataTypes.DateType
        classifier == Instant::class || classifier == java.time.Instant::class -> DataTypes.TimestampType
        classifier == String::class || classifier == Char::class -> DataTypes.StringType
        classifier == BigDecimal::class -> DataTypes.createDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)
        classifier == ByteArray::class -> DataTypes.BinaryType
        else -> kotlinScalarToSparkType(kType)
    }
}

private fun kotlinScalarToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when (classifier) {
        Int::class -> DataTypes.IntegerType
        Long::class -> DataTypes.LongType
        Double::class -> DataTypes.DoubleType
        Float::class -> DataTypes.FloatType
        Boolean::class -> DataTypes.BooleanType
        Short::class -> DataTypes.ShortType
        Byte::class -> DataTypes.ByteType
        else -> kotlinContainerOrUdtToSparkType(kType)
    }
}

private fun kotlinContainerOrUdtToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier.java.isArray || classifier.isSubclassOf(Collection::class) -> {
            val elementType = kType.arguments.first().type!!
            DataTypes.createArrayType(kotlinTypeToSparkType(elementType), elementType.isMarkedNullable)
        }

        classifier.isSubclassOf(Map::class) -> {
            val keyType = kType.arguments[0].type!!
            val valueType = kType.arguments[1].type!!
            DataTypes.createMapType(
                kotlinTypeToSparkType(keyType),
                kotlinTypeToSparkType(valueType),
                valueType.isMarkedNullable,
            )
        }

        else -> {
            requireNotNull(classifier.findAnnotation<SQLUserDefinedType>()) {
                "Unsupported type in schema inference: ${classifier.simpleName}"
            }.udt.java
                .getDeclaredConstructor()
                .newInstance() as DataType
        }
    }
}
