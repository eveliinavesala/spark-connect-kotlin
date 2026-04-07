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
import scala.collection.Map as ScalaMap
import scala.jdk.javaapi.CollectionConverters
import java.math.BigDecimal
import java.sql.Date
import java.sql.Timestamp
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.*
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

internal fun convertKotlinValueToSpark(value: Any?, declaredType: KType? = null): Any? {
    return when {
        value == null -> null
        value::class.isValue -> {
            // The value class is unwrapped via the primary constructor's backing property.
            // memberProperties.first() is alphabetical and may resolve to a computed property of the
            // same type (e.g. Duration.absoluteValue: Duration), causing infinite recursion.
            val backingName = value::class.primaryConstructor!!.parameters.first().name!!
            val property = value::class.memberProperties.find { it.name == backingName }!!
                .also { it.isAccessible = true }
            convertKotlinValueToSpark(property.call(value))
        }
        value is LocalDate -> Date.valueOf(value.toJavaLocalDate())
        value is Instant -> Timestamp.from(value.toJavaInstant())
        value is java.time.LocalDate -> Date.valueOf(value)
        value is java.time.Instant -> Timestamp.from(value)
        value::class.findAnnotation<SQLUserDefinedType>() != null -> {
            @Suppress("UNCHECKED_CAST")
            val udt = value::class.findAnnotation<SQLUserDefinedType>()!!.udt.java.getDeclaredConstructor().newInstance() as UserDefinedType<Any>
            udt.serialize(value)
        }
        value is Pair<*, *> -> ReflectionCache.getSerializer(value::class.createType(listOf(
            KTypeProjection.invariant(typeOf<Any?>()), KTypeProjection.invariant(typeOf<Any?>())
        ))).serialize(value) // Imprecise for nested generics inside Pair; adequate for non-generic cases
        value is Triple<*, *, *> -> ReflectionCache.getSerializer(value::class.createType(listOf(
            KTypeProjection.invariant(typeOf<Any?>()), KTypeProjection.invariant(typeOf<Any?>()), KTypeProjection.invariant(typeOf<Any?>())
        ))).serialize(value)
        value::class.isData || value::class.isSealed -> {
            // Use the declared KType when available (preserves generic args, e.g. Box<String>).
            // Falling back to value::class.createType() produces Box<*>, losing the type arg to erasure.
            val serializerType = declaredType ?: value::class.createType()
            ReflectionCache.getSerializer(serializerType).serialize(value)
        }
        value is Enum<*> -> value.name
        value is Char -> value.toString()
        value is Array<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it) }.toList()).toSeq()
        value is Collection<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it) }).toSeq()
        value is Map<*, *> -> CollectionConverters.asScala(value.map { (k, v) -> convertKotlinValueToSpark(k) to convertKotlinValueToSpark(v) }.toMap())
        else -> value
    }
}

@Suppress("UNCHECKED_CAST")
internal fun convertSparkValueToKotlin(value: Any?, targetType: KType): Any? {
    if (value == null) return null
    val targetClass = targetType.jvmErasure
    return when {
        targetClass.isValue -> {
            // Wrap the primitive into the value class.
            // isAccessible = true needed for value classes with internal/private constructors (e.g. Duration).
            val constructor = targetClass.primaryConstructor!!
                .also { it.isAccessible = true }
            constructor.call(value)
        }
        targetClass.isSubclassOf(Enum::class) -> {
            val enumClass = targetClass.java as Class<out Enum<*>>
            enumClass.enumConstants.first { it.name == value.toString() }
        }
        value is Date && targetClass == LocalDate::class -> value.toLocalDate().toKotlinLocalDate()
        value is Timestamp && targetClass == Instant::class -> value.toInstant().toKotlinInstant()
        value is Date && targetClass == java.time.LocalDate::class -> value.toLocalDate()
        value is Timestamp && targetClass == java.time.Instant::class -> value.toInstant()
        targetClass == Char::class && value is UTF8String -> value.toString()[0]
        targetClass == Char::class && value is String -> value[0]
        targetClass == Short::class   && value is Int        -> value.toShort()
        targetClass == Byte::class    && value is Int        -> value.toByte()
        targetClass == BigDecimal::class && value is BigDecimal -> value
        targetClass == BigDecimal::class                     -> BigDecimal(value.toString())
        value is UTF8String -> value.toString()
        value is Seq<*> -> {
            val elementType = targetType.arguments.first().type!!
            val kotlinList = CollectionConverters.asJava(value).map { convertSparkValueToKotlin(it, elementType) }
            when {
                targetClass.java.isArray && targetClass != ByteArray::class -> {
                    // A properly-typed component array (e.g. String[]) is required.
                    // Object[] is rejected by Kotlin reflection's callBy with an argument type mismatch.
                    val componentType = elementType.jvmErasure.java
                    val arr = java.lang.reflect.Array.newInstance(componentType, kotlinList.size)
                    kotlinList.forEachIndexed { i, v -> java.lang.reflect.Array.set(arr, i, v) }
                    arr
                }
                targetClass.isSubclassOf(Set::class) -> kotlinList.toSet()
                else -> kotlinList
            }
        }
        value is ScalaMap<*, *> -> {
            val valueType = targetType.arguments[1].type!!
            CollectionConverters.asJava(value).mapValues { (_, v) -> convertSparkValueToKotlin(v, valueType) }
        }
        value is Row -> {
            val udtAnnotation = targetClass.findAnnotation<SQLUserDefinedType>()
            if (udtAnnotation != null) {
                val udt = udtAnnotation.udt.java.getDeclaredConstructor().newInstance() as UserDefinedType<Any>
                udt.deserialize(value)
            } else if (targetClass.isData || targetClass == Pair::class || targetClass == Triple::class) {
                ReflectionCache.getDeserializer<Any>(targetType).deserialize(value)
            } else {
                value
            }
        }
        else -> value
    }
}
