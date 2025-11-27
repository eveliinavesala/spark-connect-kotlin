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
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure

fun <T : Any> SparkSession.createPragmaticDataFrame(data: List<T>, kClass: KClass<T>): Dataset<Row> {
    if (data.isEmpty()) return this.emptyDataFrame()
    val schema = inferSchema(kClass)
    val rows = data.map { obj ->
        val values = kClass.memberProperties.map { prop ->
            val kotlinValue = prop.get(obj)
            convertKotlinValueToScala(kotlinValue)
        }.toTypedArray()
        GenericRowWithSchema(values, schema) as Row
    }
    return this.createDataFrame(rows, schema)
}

fun <T : Any> Dataset<Row>.toKotlinList(kClass: KClass<T>): List<T> {
    val constructor = kClass.primaryConstructor ?: error("No primary constructor for ${kClass.simpleName}")
    val collectedRows: List<Row> = this.collectAsList()

    return collectedRows.map { row ->
        val argsMap = constructor.parameters.associateWith { param ->
            val rawValue: Any? = row.getAs(param.name)
            convertSparkValueToKotlin(rawValue, param.type)
        }
        constructor.callBy(argsMap)
    }
}

private fun convertKotlinValueToScala(value: Any?): Any? {
    return when (value) {
        null -> null
        is Set<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToScala(it) }).toSeq()
        is List<*> -> CollectionConverters.asScala(value.map { convertKotlinValueToScala(it) }).toSeq()
        is Map<*, *> -> {
            val scalaMap = value.map { (k, v) ->
                convertKotlinValueToScala(k) to convertKotlinValueToScala(v)
            }.toMap()
            CollectionConverters.asScala(scalaMap)
        }
        // This version does NOT handle nested data classes for serialization.
        else -> value
    }
}

private fun convertSparkValueToKotlin(value: Any?, targetType: KType): Any? {
    if (value == null) return null
    val targetClass = targetType.jvmErasure

    return when (value) {
        is UTF8String -> value.toString()
        is Seq<*> -> {
            val elementType = targetType.arguments.first().type!!
            val javaList = CollectionConverters.asJava(value)
            val kotlinList = javaList.map { convertSparkValueToKotlin(it, elementType) }
            if (targetClass.isSubclassOf(Set::class)) kotlinList.toSet() else kotlinList
        }
        is ScalaMap<*, *> -> {
            val valueType = targetType.arguments[1].type!!
            val javaMap = CollectionConverters.asJava(value)
            javaMap.mapValues { (_, v) -> convertSparkValueToKotlin(v, valueType) }
        }
        // This version does NOT handle nested data classes for deserialization.
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
