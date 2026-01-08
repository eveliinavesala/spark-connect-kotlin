package integration_pack

import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaInstant
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toKotlinInstant
import kotlinx.datetime.toKotlinLocalDate
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.*
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.Seq
import scala.collection.Map as ScalaMap
import scala.jdk.javaapi.CollectionConverters
import java.util.AbstractList
import java.util.concurrent.ConcurrentHashMap
import java.sql.Date
import java.sql.Timestamp
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

// ============================================================================
// Public API
// ============================================================================

inline fun <reified T : Any> List<T>.toDataFrame(spark: SparkSession): Dataset<Row> {
    return spark.createDataFrameFromKotlinList(this, typeOf<T>())
}

inline fun <reified T : Any> Dataset<Row>.toKotlinList(): List<T> {
    return this.toKotlinListFromDataFrame(typeOf<T>())
}

fun <T : Any> SparkSession.createDataFrameFromKotlinList(data: List<T>, kType: KType): Dataset<Row> {
    return createDataFrameViaReflectionInternal(data, kType)
}

fun <T : Any> Dataset<Row>.toKotlinListFromDataFrame(kType: KType): List<T> {
    return toKotlinClassListInternal<T>(kType)
}

// ============================================================================
// Reflection Cache & Internals
// ============================================================================

internal object ReflectionCache {
    // Cache keys changed from KClass to KType to support generics (e.g. Pair<String, Int> vs Pair<Int, Int>)
    private val schemaCache = ConcurrentHashMap<KType, StructType>()
    private val serializerCache = ConcurrentHashMap<KType, RowSerializer>()
    private val deserializerCache = ConcurrentHashMap<KType, RowDeserializer<*>>()
    
    fun getSchema(kType: KType): StructType = schemaCache.getOrPut(kType) { inferSchemaInternal(kType) }
    fun getSerializer(kType: KType): RowSerializer = serializerCache.getOrPut(kType) { RowSerializer.create(kType) }
    @Suppress("UNCHECKED_CAST")
    fun <T : Any> getDeserializer(kType: KType): RowDeserializer<T> = deserializerCache.getOrPut(kType) { RowDeserializer.create<T>(kType) } as RowDeserializer<T>
}

internal class RowSerializer private constructor(
    private val schema: StructType,
    private val fieldSerializers: List<FieldSerializer>
) {
    fun serialize(obj: Any): Row {
        val values = fieldSerializers.map { it.extract(obj) }.toTypedArray()
        return GenericRowWithSchema(values, schema)
    }
    
    private class FieldSerializer(val fieldName: String, val property: KProperty1<out Any, *>?) {
        fun extract(obj: Any): Any? {
            return when {
                fieldName == "_type" -> obj::class.simpleName
                // Special handling for Pair/Triple mapping _1 -> first, etc.
                obj is Pair<*, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Pair<*, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Triple<*, *, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_3" -> convertKotlinValueToSpark(obj.third)
                property != null -> convertKotlinValueToSpark(property.call(obj))
                else -> error("Property '$fieldName' not found on object of type ${obj::class.simpleName}")
            }
        }
    }
    
    companion object {
        fun create(kType: KType): RowSerializer {
            val kClass = kType.jvmErasure
            val schema = ReflectionCache.getSchema(kType)
            val fieldSerializers = schema.fields().map { field ->
                // For Pair/Triple, we don't need to find the property by name "_1", we handle it in extract
                val prop = if (kClass == Pair::class || kClass == Triple::class) {
                    null 
                } else {
                    kClass.memberProperties.find { it.name == field.name() }
                        ?: if (field.name() == "_type" && kClass.isSealed) null 
                           else error("Property '${field.name()}' not found in ${kClass.simpleName}")
                }
                FieldSerializer(field.name(), prop)
            }
            return RowSerializer(schema, fieldSerializers)
        }
    }
}

internal class RowDeserializer<T : Any> private constructor(
    private val constructor: kotlin.reflect.KFunction<T>,
    private val parameterExtractors: List<ParameterExtractor>
) {
    fun deserialize(row: Row): T {
        val argsMap = parameterExtractors.associate { extractor ->
            extractor.parameter to extractor.extract(row)
        }
        return constructor.callBy(argsMap)
    }
    
    private class ParameterExtractor(val parameter: KParameter, val paramName: String, val paramType: KType) {
        fun extract(row: Row): Any? {
            val rawValue: Any? = row.getAs(paramName)
            if (rawValue == null && !parameter.type.isMarkedNullable) {
                throw IllegalArgumentException("Null value received for non-nullable parameter '$paramName'")
            }
            return convertSparkValueToKotlin(rawValue, paramType)
        }
    }
    
    companion object {
        fun <T : Any> create(kType: KType): RowDeserializer<T> {
            @Suppress("UNCHECKED_CAST")
            val kClass = kType.jvmErasure as KClass<T>
            val constructor = kClass.primaryConstructor ?: error("No primary constructor for ${kClass.simpleName}")
            val extractors = constructor.parameters.map { param ->
                val paramName = when {
                    kClass == Pair::class && param.index == 0 -> "_1"
                    kClass == Pair::class && param.index == 1 -> "_2"
                    kClass == Triple::class && param.index == 0 -> "_1"
                    kClass == Triple::class && param.index == 1 -> "_2"
                    kClass == Triple::class && param.index == 2 -> "_3"
                    else -> param.name!!
                }
                // For generics, we need to resolve the type from the KType arguments
                val resolvedType = if (kClass.typeParameters.isNotEmpty()) {
                     // Simple resolution for Pair/Triple where type params map 1:1 to constructor params
                     if (kClass == Pair::class || kClass == Triple::class) {
                         kType.arguments[param.index].type!!
                     } else {
                         // Fallback for other generics (complex to implement fully without a type resolver)
                         param.type 
                     }
                } else {
                    param.type
                }
                ParameterExtractor(param, paramName, resolvedType)
            }
            return RowDeserializer(constructor, extractors)
        }
    }
}

internal class LazyRowList(private val sourceData: List<Any>, private val serializer: RowSerializer) : AbstractList<Row>() {
    override fun get(index: Int): Row = serializer.serialize(sourceData[index])
    override val size: Int get() = sourceData.size
}

internal fun SparkSession.createDataFrameViaReflectionInternal(data: List<Any>, kType: KType): Dataset<Row> {
    val schema = ReflectionCache.getSchema(kType)
    if (data.isEmpty()) {
        return this.createDataFrame(emptyList<Row>(), schema)
    }
    val serializer = ReflectionCache.getSerializer(kType)
    return this.createDataFrame(LazyRowList(data, serializer), schema)
}

internal fun <T : Any> Dataset<Row>.toKotlinClassListInternal(kType: KType): List<T> {
    val collectedRows: List<Row> = this.collectAsList()
    val kClass = kType.jvmErasure
    
    if (kClass.isSealed) {
        return collectedRows.map { row ->
            val typeName = row.getAs<String>("_type")
            val subClass = kClass.sealedSubclasses.find { it.simpleName == typeName } ?: error("Unknown subclass type '$typeName'")
            // Note: Sealed subclasses usually aren't generic in this context, or we'd need more complex logic
            @Suppress("UNCHECKED_CAST")
            ReflectionCache.getDeserializer<Any>(subClass.createType()).deserialize(row) as T
        }
    }
    val deserializer = ReflectionCache.getDeserializer<T>(kType)
    return collectedRows.map { deserializer.deserialize(it) }
}

private fun inferSchemaInternal(kType: KType): StructType {
    val kClass = kType.jvmErasure
    
    if (kClass == Pair::class) {
        val firstType = kType.arguments[0].type!!
        val secondType = kType.arguments[1].type!!
        return StructType(arrayOf(
            StructField("_1", kotlinTypeToSparkType(firstType), firstType.isMarkedNullable, Metadata.empty()),
            StructField("_2", kotlinTypeToSparkType(secondType), secondType.isMarkedNullable, Metadata.empty())
        ))
    }
    
    if (kClass == Triple::class) {
        val firstType = kType.arguments[0].type!!
        val secondType = kType.arguments[1].type!!
        val thirdType = kType.arguments[2].type!!
        return StructType(arrayOf(
            StructField("_1", kotlinTypeToSparkType(firstType), firstType.isMarkedNullable, Metadata.empty()),
            StructField("_2", kotlinTypeToSparkType(secondType), secondType.isMarkedNullable, Metadata.empty()),
            StructField("_3", kotlinTypeToSparkType(thirdType), thirdType.isMarkedNullable, Metadata.empty())
        ))
    }

    val fields = if (kClass.isSealed) {
        (kClass.sealedSubclasses.flatMap { it.memberProperties }.distinctBy { it.name }.map {
            StructField(it.name, kotlinTypeToSparkType(it.returnType), true, Metadata.empty())
        } + StructField("_type", DataTypes.StringType, false, Metadata.empty()))
    } else {
        kClass.memberProperties.map {
            StructField(it.name, kotlinTypeToSparkType(it.returnType), it.returnType.isMarkedNullable, Metadata.empty())
        }
    }
    return StructType(fields.toTypedArray())
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

internal fun convertKotlinValueToSpark(value: Any?): Any? {
    return when {
        value == null -> null
        value::class.isValue -> {
            // Unwrap the value class
            val property = value::class.memberProperties.first()
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
        ))).serialize(value) // Note: This is imperfect for nested generics in Pair, but works for simple cases
        value is Triple<*, *, *> -> ReflectionCache.getSerializer(value::class.createType(listOf(
            KTypeProjection.invariant(typeOf<Any?>()), KTypeProjection.invariant(typeOf<Any?>()), KTypeProjection.invariant(typeOf<Any?>())
        ))).serialize(value)
        value::class.isData || value::class.isSealed -> ReflectionCache.getSerializer(value::class.createType()).serialize(value)
        value is Enum<*> -> value.name
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
            // Wrap the primitive into the value class
            val constructor = targetClass.primaryConstructor!!
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
        value is UTF8String -> value.toString()
        value is Seq<*> -> {
            val elementType = targetType.arguments.first().type!!
            val kotlinList = CollectionConverters.asJava(value).map { convertSparkValueToKotlin(it, elementType) }
            if (targetClass.isSubclassOf(Set::class)) kotlinList.toSet() else kotlinList
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
