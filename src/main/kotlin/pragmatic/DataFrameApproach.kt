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
import kotlin.reflect.full.*
import kotlin.reflect.jvm.jvmErasure

fun <T : Any> SparkSession.createPragmaticDataFrame(data: List<T>, kClass: KClass<T>): Dataset<Row> {
    if (data.isEmpty()) return this.emptyDataFrame()
    val schema = inferSchema(kClass)
    val rows = data.map { obj ->
        convertKotlinObjectToRow(obj, schema)
    }
    return this.createDataFrame(rows, schema)
}

@Suppress("UNCHECKED_CAST")
private fun <T : Any> convertSpecificObjectToRow(obj: T, schema: StructType): Row {
    val kClass = obj::class as KClass<T>
    
    val values = schema.fields().map { field ->
        when (field.name()) {
            "_type" -> kClass.simpleName
            else -> {
                val prop = kClass.memberProperties.find { it.name == field.name() }
                if (prop != null) {
                    val kotlinValue = prop.get(obj)
                    convertKotlinValueToSpark(kotlinValue)
                } else {
                    null // This field does not exist in this subclass
                }
            }
        }
    }.toTypedArray()

    return GenericRowWithSchema(values, schema)
}

private fun convertKotlinObjectToRow(obj: Any, schema: StructType): Row {
    return convertSpecificObjectToRow(obj, schema)
}

private fun convertKotlinValueToSpark(value: Any?): Any? {
    return when {
        value == null -> null
        value::class.isSealed -> convertKotlinObjectToRow(value, inferSchema(value::class))
        value::class.isData -> convertKotlinObjectToRow(value, inferSchema(value::class))
        value is Enum<*> -> value.name
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
    val collectedRows: List<Row> = this.collectAsList()

    if (kClass.isSealed) {
        return collectedRows.map { row ->
            val typeName = row.getAs<String>("_type")
            val subClass = kClass.sealedSubclasses.find { it.simpleName == typeName }
                ?: error("Unknown subclass type '$typeName' for sealed class '${kClass.simpleName}'")
            createObjectFromRow(row, subClass)
        }
    }

    return collectedRows.map { row ->
        createObjectFromRow(row, kClass)
    }
}

private fun <T : Any> createObjectFromRow(row: Row, kClass: KClass<T>): T {
    val constructor = kClass.primaryConstructor ?: error("No primary constructor for ${kClass.simpleName}")
    val argsMap = constructor.parameters.associateWith { param ->
        val rawValue: Any? = row.getAs(param.name)
        if (rawValue == null && !param.type.isMarkedNullable) {
            throw IllegalArgumentException(
                "Null value received from Spark for non-nullable parameter '${param.name}' in ${kClass.simpleName}"
            )
        }
        convertSparkValueToKotlin(rawValue, param.type)
    }
    return constructor.callBy(argsMap)
}

private fun convertSparkValueToKotlin(value: Any?, targetType: KType): Any? {
    if (value == null) return null
    val targetClass = targetType.jvmErasure

    return when {
        targetClass.isSubclassOf(Enum::class) -> {
            @Suppress("UNCHECKED_CAST")
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
        value is Row && targetClass.isData -> {
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
    if (kClass.isSealed) {
        val allProperties = kClass.sealedSubclasses.flatMap { it.memberProperties }.distinctBy { it.name }
        val fields = allProperties.map { prop ->
            StructField(prop.name, kotlinTypeToSparkType(prop.returnType), true, Metadata.empty()) // All fields must be nullable
        }
        val typeField = StructField("_type", DataTypes.StringType, false, Metadata.empty())
        return StructType((fields + typeField).toTypedArray())
    }

    val fields = kClass.memberProperties.map { prop ->
        StructField(prop.name, kotlinTypeToSparkType(prop.returnType), prop.returnType.isMarkedNullable, Metadata.empty())
    }
    return StructType(fields.toTypedArray())
}

private fun kotlinTypeToSparkType(kType: KType): DataType {
    val classifier = kType.jvmErasure
    return when {
        classifier.isSubclassOf(Enum::class) -> DataTypes.StringType
        classifier.isSealed -> inferSchema(classifier)
        classifier.isData -> inferSchema(classifier)
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
        else -> throw IllegalArgumentException("Unsupported type in schema inference: ${classifier.simpleName}")
    }
}
