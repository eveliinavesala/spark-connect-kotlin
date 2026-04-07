package spark.kotlin.reflect

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import org.apache.spark.sql.types.*
import java.math.BigDecimal
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

// Tracks types currently being schema-inferred on each thread to detect recursive definitions.
// Nullability is stripped before lookup so Tree and Tree? are treated as the same type.
private val schemaInProgress = ThreadLocal.withInitial<MutableSet<KType>> { mutableSetOf() }

internal fun inferSchemaInternal(kType: KType): StructType {
    val normalizedType = kType.jvmErasure.createType(kType.arguments, nullable = false)
    val inProgress = schemaInProgress.get()
    if (normalizedType in inProgress) throw IllegalArgumentException(
        "Recursive type detected: '${kType.jvmErasure.simpleName}' contains a direct or indirect " +
        "reference to itself. Recursive schemas are not supported — flatten the type or use a UDT."
    )
    inProgress.add(normalizedType)
    try {

    val kClass = kType.jvmErasure

    if (kClass == Pair::class || kClass == Triple::class) {
        val fields = kType.arguments.mapIndexed { index, arg ->
            val type = arg.type!!
            StructField("_" + (index + 1), kotlinTypeToSparkType(type), type.isMarkedNullable, Metadata.empty())
        }
        return StructType(fields.toTypedArray())
    }

    val fields = if (kClass.isSealed) {
        // Collect all concrete leaf subclasses across the full hierarchy (including intermediate sealed levels)
        val leaves = kClass.allLeafSubclasses()
        // Check for simpleName collisions across the full hierarchy
        val simpleNames = leaves.map { it.simpleName }
        if (simpleNames.distinct().size != simpleNames.size) {
            val duplicates = simpleNames.groupingBy { it }.eachCount().filter { it.value > 1 }.keys
            throw IllegalArgumentException(
                "Sealed class hierarchy '${kClass.simpleName}' contains subclasses with duplicate simple names: $duplicates. " +
                "This causes ambiguity in serialization. Please rename subclasses to be unique."
            )
        }

        (leaves.flatMap { it.memberProperties }.distinctBy { it.name }.map {
            StructField(it.name, kotlinTypeToSparkType(it.returnType), true, Metadata.empty())
        } + StructField("_type", DataTypes.StringType, false, Metadata.empty()))
    } else {
        val argMap = buildTypeArgMap(kType)
        kClass.memberProperties.map {
            val resolvedType = resolveTypeParam(it.returnType, argMap)
            StructField(it.name, kotlinTypeToSparkType(resolvedType), resolvedType.isMarkedNullable, Metadata.empty())
        }
    }
    return StructType(fields.toTypedArray())

    } finally {
        inProgress.remove(normalizedType)
    }
}

internal fun kotlinTypeToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier.isSubclassOf(Enum::class) -> DataTypes.StringType
        classifier.isSealed -> ReflectionCache.getSchema(kType)
        classifier.isData -> ReflectionCache.getSchema(kType)
        classifier == Pair::class -> ReflectionCache.getSchema(kType)
        classifier == Triple::class -> ReflectionCache.getSchema(kType)
        classifier.isValue -> {
            val underlyingProperty = classifier.primaryConstructor!!.parameters.first().type
            kotlinTypeToSparkType(underlyingProperty)
        }
        classifier == LocalDate::class || classifier == java.time.LocalDate::class -> DataTypes.DateType
        classifier == Instant::class || classifier == java.time.Instant::class -> DataTypes.TimestampType
        classifier == String::class -> DataTypes.StringType
        classifier == Int::class -> DataTypes.IntegerType
        classifier == Long::class -> DataTypes.LongType
        classifier == Double::class -> DataTypes.DoubleType
        classifier == Float::class -> DataTypes.FloatType
        classifier == Boolean::class -> DataTypes.BooleanType
        classifier == Short::class      -> DataTypes.ShortType
        classifier == Byte::class       -> DataTypes.ByteType
        classifier == Char::class       -> DataTypes.StringType
        classifier == BigDecimal::class -> DataTypes.createDecimalType(38, 18)
        classifier == ByteArray::class -> DataTypes.BinaryType
        classifier.java.isArray -> {
            // Array<T> — ordered before the Collection check to take precedence; ByteArray is handled above
            val elementType = kType.arguments.first().type!!
            DataTypes.createArrayType(kotlinTypeToSparkType(elementType), elementType.isMarkedNullable)
        }
        classifier.isSubclassOf(Collection::class) -> {
            val elementType = kType.arguments.first().type!!
            DataTypes.createArrayType(kotlinTypeToSparkType(elementType), elementType.isMarkedNullable)
        }
        classifier.isSubclassOf(Map::class) -> {
            val keyType = kType.arguments[0].type!!
            val valueType = kType.arguments[1].type!!
            DataTypes.createMapType(kotlinTypeToSparkType(keyType), kotlinTypeToSparkType(valueType), valueType.isMarkedNullable)
        }
        else -> {
            val udtAnnotation = classifier.findAnnotation<SQLUserDefinedType>()
            if (udtAnnotation != null) {
                udtAnnotation.udt.java.getDeclaredConstructor().newInstance() as DataType
            } else {
                throw IllegalArgumentException("Unsupported type in schema inference: ${classifier.simpleName}")
            }
        }
    }
}
