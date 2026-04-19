package spark.kotlin.reflect

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toKotlinInstant
import kotlinx.datetime.toKotlinLocalDate
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.types.UserDefinedType
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.Seq
import scala.jdk.javaapi.CollectionConverters
import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf
import scala.collection.Map as ScalaMap

/**
 * Converts a Kotlin value to its Spark-compatible representation.
 */
internal fun convertKotlinValueToSpark(
    value: Any?,
    declaredType: KType? = null,
): Any? =
    when {
        value == null -> null
        value::class.isValue -> convertValueClassToSpark(value)
        value is Enum<*> -> value.name
        value is Char -> value.toString()
        else -> convertKotlinStructuralToSpark(value, declaredType)
    }

private fun convertKotlinStructuralToSpark(
    value: Any,
    declaredType: KType?,
): Any? =
    when (value) {
        is LocalDate, is java.time.LocalDate, is Instant, is java.time.Instant -> {
            convertDateTimeToSpark(value)
        }

        is Array<*>, is Collection<*>, is Map<*, *> -> {
            convertCollectionToSpark(value, declaredType)
        }

        else -> {
            convertKotlinComplexToSpark(value, declaredType)
        }
    }

private fun convertKotlinComplexToSpark(
    value: Any,
    declaredType: KType?,
): Any? =
    when {
        value::class.findAnnotation<SQLUserDefinedType>() != null -> {
            convertUdtToSpark(value)
        }

        value is Pair<*, *> || value is Triple<*, *, *> || value::class.isData || value::class.isSealed -> {
            convertComplexTypeToSpark(value, declaredType)
        }

        else -> {
            value
        }
    }

private fun convertValueClassToSpark(value: Any): Any? {
    val backingName =
        value::class
            .primaryConstructor!!
            .parameters
            .first()
            .name!!
    val property =
        value::class
            .memberProperties
            .find { it.name == backingName }!!
            .also { it.isAccessible = true }
    return convertKotlinValueToSpark(property.call(value))
}

private fun convertDateTimeToSpark(value: Any): Any =
    when (value) {
        is LocalDate -> Date.valueOf(value.toJavaLocalDate())
        is Instant -> Timestamp.from(value.toJavaInstant())
        is java.time.LocalDate -> Date.valueOf(value)
        is java.time.Instant -> Timestamp.from(value)
        else -> error("Unsupported date-time type: ${value::class}")
    }

private fun convertUdtToSpark(value: Any): Any? {
    @Suppress("UNCHECKED_CAST")
    val udt =
        value::class
            .findAnnotation<SQLUserDefinedType>()!!
            .udt.java
            .getDeclaredConstructor()
            .newInstance() as UserDefinedType<Any>
    return udt.serialize(value)
}

private fun convertComplexTypeToSpark(
    value: Any,
    declaredType: KType?,
): Any =
    when (value) {
        is Pair<*, *> -> {
            ReflectionCache
                .getSerializer(
                    value::class.createType(
                        listOf(
                            KTypeProjection.invariant(typeOf<Any?>()),
                            KTypeProjection.invariant(typeOf<Any?>()),
                        ),
                    ),
                ).serialize(value)
        }

        is Triple<*, *, *> -> {
            ReflectionCache
                .getSerializer(
                    value::class.createType(
                        listOf(
                            KTypeProjection.invariant(typeOf<Any?>()),
                            KTypeProjection.invariant(typeOf<Any?>()),
                            KTypeProjection.invariant(typeOf<Any?>()),
                        ),
                    ),
                ).serialize(value)
        }

        else -> {
            val serializerType = declaredType ?: value::class.createType()
            ReflectionCache.getSerializer(serializerType).serialize(value)
        }
    }

private fun convertCollectionToSpark(
    value: Any,
    declaredType: KType?,
): Any =
    when (value) {
        is Array<*> -> {
            val elementType = declaredType?.arguments?.firstOrNull()?.type
            CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it, elementType) }.toList()).toSeq()
        }

        is Collection<*> -> {
            val elementType = declaredType?.arguments?.firstOrNull()?.type
            CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it, elementType) }).toSeq()
        }

        is Map<*, *> -> {
            val keyType = declaredType?.arguments?.getOrNull(0)?.type
            val valueType = declaredType?.arguments?.getOrNull(1)?.type
            CollectionConverters.asScala(
                value
                    .map { (k, v) ->
                        convertKotlinValueToSpark(k, keyType) to convertKotlinValueToSpark(v, valueType)
                    }.toMap(),
            )
        }

        else -> {
            error("Unsupported collection type: ${value::class}")
        }
    }

/**
 * Converts a Spark row value to the Kotlin type specified by [targetType].
 */
@Suppress("UNCHECKED_CAST")
internal fun convertSparkValueToKotlin(
    value: Any?,
    targetType: KType,
): Any? {
    if (value == null) return null
    val targetClass = targetType.jvmErasure
    return when {
        targetClass.isValue -> convertValueClassToKotlin(value, targetClass)
        targetClass.isSubclassOf(Enum::class) -> convertEnumToKotlin(value, targetClass)
        value is Date || value is Timestamp -> convertDateTimeToKotlin(value, targetClass)
        value is Seq<*> || value is ScalaMap<*, *> -> convertContainerToKotlin(value, targetType, targetClass)
        value is Row -> convertRowToKotlin(value, targetType, targetClass)
        else -> convertPrimitiveToKotlin(value, targetClass)
    }
}

private fun convertContainerToKotlin(
    value: Any,
    targetType: KType,
    targetClass: kotlin.reflect.KClass<*>,
): Any? =
    when (value) {
        is Seq<*> -> convertSeqToKotlin(value, targetType, targetClass)
        is ScalaMap<*, *> -> convertMapToKotlin(value, targetType)
        else -> value
    }

private fun convertPrimitiveToKotlin(
    value: Any,
    targetClass: kotlin.reflect.KClass<*>,
): Any =
    when {
        targetClass == Char::class -> {
            convertToChar(value)
        }

        targetClass == Short::class || targetClass == Byte::class || targetClass == BigDecimal::class -> {
            convertNumericToKotlin(value, targetClass)
        }

        value is UTF8String -> {
            value.toString()
        }

        else -> {
            value
        }
    }

private fun convertValueClassToKotlin(
    value: Any,
    targetClass: kotlin.reflect.KClass<*>,
): Any {
    val constructor =
        targetClass.primaryConstructor!!
            .also { it.isAccessible = true }
    return constructor.call(value)
}

private fun convertEnumToKotlin(
    value: Any,
    targetClass: kotlin.reflect.KClass<*>,
): Any? {
    val enumClass = targetClass.java as Class<out Enum<*>>
    return enumClass.enumConstants.first { it.name == value.toString() }
}

private fun convertDateTimeToKotlin(
    value: Any,
    targetClass: kotlin.reflect.KClass<*>,
): Any? =
    when (value) {
        is Date if targetClass == LocalDate::class -> value.toLocalDate().toKotlinLocalDate()
        is Timestamp if targetClass == Instant::class -> value.toInstant().toKotlinInstant()
        is Date if targetClass == java.time.LocalDate::class -> value.toLocalDate()
        is Timestamp if targetClass == java.time.Instant::class -> value.toInstant()
        else -> value
    }

private fun convertToChar(value: Any): Char =
    when (value) {
        is UTF8String -> value.toString()[0]
        is String -> value[0]
        else -> error("Cannot convert ${value::class} to Char")
    }

private fun convertNumericToKotlin(
    value: Any,
    targetClass: kotlin.reflect.KClass<*>,
): Any =
    when (targetClass) {
        Short::class if value is Int -> value.toShort()
        Byte::class if value is Int -> value.toByte()
        BigDecimal::class if value is BigDecimal -> value
        BigDecimal::class -> BigDecimal(value.toString())
        else -> value
    }

private fun convertSeqToKotlin(
    value: Seq<*>,
    targetType: KType,
    targetClass: kotlin.reflect.KClass<*>,
): Any? {
    val elementType = targetType.arguments.first().type!!
    val kotlinList = CollectionConverters.asJava(value).map { convertSparkValueToKotlin(it, elementType) }
    return when {
        targetClass.java.isArray && targetClass != ByteArray::class -> {
            val componentType = elementType.jvmErasure.java
            val arr =
                java.lang.reflect.Array
                    .newInstance(componentType, kotlinList.size)
            kotlinList.forEachIndexed { i, v ->
                java.lang.reflect.Array
                    .set(arr, i, v)
            }
            arr
        }

        targetClass.isSubclassOf(Set::class) -> {
            kotlinList.toSet()
        }

        else -> {
            kotlinList
        }
    }
}

private fun convertMapToKotlin(
    value: ScalaMap<*, *>,
    targetType: KType,
): Map<Any?, Any?> {
    val valueType = targetType.arguments[1].type!!
    return CollectionConverters.asJava(value).mapValues { (_, v) -> convertSparkValueToKotlin(v, valueType) }
}

private fun convertRowToKotlin(
    value: Row,
    targetType: KType,
    targetClass: kotlin.reflect.KClass<*>,
): Any? {
    val udtAnnotation = targetClass.findAnnotation<SQLUserDefinedType>()
    return when {
        udtAnnotation != null -> {
            val udt =
                udtAnnotation.udt.java
                    .getDeclaredConstructor()
                    .newInstance() as UserDefinedType<Any>
            udt.deserialize(value)
        }

        targetClass.isData || targetClass == Pair::class || targetClass == Triple::class -> {
            ReflectionCache.getDeserializer<Any>(targetType).deserialize(value)
        }

        targetClass.isSealed -> {
            val typeName = value.getAs<String>("_type")
            val subClass =
                targetClass.allLeafSubclasses().find { it.simpleName == typeName }
                    ?: error("Unknown subclass type '$typeName' for sealed class '${targetClass.simpleName}'")
            ReflectionCache.getDeserializer<Any>(subClass.createType()).deserialize(value)
        }

        else -> {
            value
        }
    }
}
