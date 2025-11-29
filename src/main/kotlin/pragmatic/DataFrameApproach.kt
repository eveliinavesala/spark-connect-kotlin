package pragmatic

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.*
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.Seq
import scala.collection.Map as ScalaMap
import scala.jdk.javaapi.CollectionConverters

import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.isData
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

fun <T : Any> SparkSession.createPragmaticDataFrame(data: List<T>, kClass: KClass<T>): Dataset<Row> {
    if (data.isEmpty()) return this.emptyDataFrame()
    val schema = inferSchema(kClass)
    val rows = data.map { obj ->
        convertKotlinObjectToRow(obj, schema)
    }
    return this.createDataFrame(rows, schema)
}

private fun convertKotlinObjectToRow(obj: Any, schema: StructType): Row {
    val kClass = obj::class
    val values = kClass.memberProperties.map { prop ->
        val kotlinValue = prop.get(obj)
        convertKotlinValueToSpark(kotlinValue)
    }.toTypedArray()
    return GenericRowWithSchema(values, schema)
}

private fun convertKotlinValueToSpark(value: Any?): Any? {
    return when {
        value == null -> null
        value is Enum<*> -> value.name // Convert Enum to String
        value::class.isData -> convertKotlinObjectToRow(value, inferSchema(value::class))
        value is Set<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it) }).toSeq()
        value is List<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToSpark(it) }).toSeq()
        value is Map<*, *> -> {
            val scalaMap = value.map { (k, v) ->
                convertKotlinValueToSpark(k) to convertKotlinValueToSpark(v)
            }.toMap()
            CollectionConverters.asScala(scalaMap)
        }
        else -> value
    }
}

fun <T : Any> Dataset<Row>.toKotlinList(kClass: KClass<T>): List<T> {
    val constructor = kClass.primaryConstructor ?: error("No primary constructor for ${kClass.simpleName}")
    val collectedRows: List<Row> = this.collectAsList()

    return collectedRows.map { row ->
        val argsMap = constructor.parameters.associateWith { param ->
            val rawValue: Any? = row.getAs(param.name)
            if (rawValue == null && !param.type.isMarkedNullable) {
                throw IllegalArgumentException(
                    "Null value received from Spark for non-nullable parameter '${param.name}' in ${kClass.simpleName}"
                )
            }
            convertSparkValueToKotlin(rawValue, param.type)
        }
        constructor.callBy(argsMap)
    }
}

private fun convertSparkValueToKotlin(value: Any?, targetType: KType): Any? {
    if (value == null) return null
    val targetClass = targetType.jvmErasure

    return when {
        targetClass.isSubclassOf(Enum::class) -> {
            // Convert String back to Enum
            val enumClass = targetClass.java as Class<out Enum<*>>
            enumClass.enumConstants.first { (it as Enum<*>).name == value.toString() }
        }
        value is UTF8String -> value.toString()
        value is Seq<*> -> {
            val elementType = targetType.arguments.first().type!!
            val javaList = CollectionConverters.asJava(value)
            val kotlinList = javaList.map { convertSparkValueToKotlin(it, elementType) }
            if (targetClass.isSubclassOf(Set::class)) kotlinList.toSet() else kotlinList
        }
        value is ScalaMap<*, *> -> {
            val valueType = targetType.arguments[1].type!!
            val javaMap = CollectionConverters.asJava(value)
            javaMap.mapValues { (_, v) -> convertSparkValueToKotlin(v, valueType) }
        }
        value is Row -> {
            val nestedConstructor = targetClass.primaryConstructor!!
            val nestedArgs = nestedConstructor.parameters.associateWith { param ->
                convertSparkValueToKotlin(value.getAs(param.name), param.type)
            }
            nestedConstructor.callBy(nestedArgs)
        }
        else -> value
    }
}

private fun inferSchema(kClass: KClass<*>): StructType {
    val fields = kClass.memberProperties.map { prop ->
        StructField(prop.name, kotlinTypeToSparkType(prop.returnType), prop.returnType.isMarkedNullable, Metadata.empty())
    }
    return StructType(fields.toTypedArray())
}

private fun kotlinTypeToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier.isSubclassOf(Enum::class) -> DataTypes.StringType // Enums are stored as Strings
        classifier == String::class -> DataTypes.StringType
        classifier == Int::class -> DataTypes.IntegerType
        classifier == Long::class -> DataTypes.LongType
        classifier == Double::class -> DataTypes.DoubleType
        classifier == Float::class -> DataTypes.FloatType
        classifier == Boolean::class -> DataTypes.BooleanType
        classifier.isSubclassOf(Collection::class) -> {
            val elementType = kType.arguments.firstOrNull()?.type ?: error("Collection type needs a generic argument.")
            DataTypes.createArrayType(kotlinTypeToSparkType(elementType), elementType.isMarkedNullable)
        }
        classifier.isSubclassOf(Map::class) -> {
            val keyType = kType.arguments.getOrNull(0)?.type ?: error("Map needs a key type.")
            val valueType = kType.arguments.getOrNull(1)?.type ?: error("Map needs a value type.")
            DataTypes.createMapType(kotlinTypeToSparkType(keyType), kotlinTypeToSparkType(valueType), valueType.isMarkedNullable)
        }
        classifier.isData -> inferSchema(classifier)
        else -> throw IllegalArgumentException("Unsupported type in schema inference: ${classifier.simpleName}")
    }
}
