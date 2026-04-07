package spark.kotlin.reflect

import org.apache.spark.sql.Row
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure

internal class RowDeserializer<T : Any> private constructor(
    private val constructor: kotlin.reflect.KFunction<T>?,
    private val parameterExtractors: List<ParameterExtractor>,
    private val singleton: T? = null
) {
    fun deserialize(row: Row): T {
        if (singleton != null) return singleton
        val argsMap = parameterExtractors.associate { extractor ->
            extractor.parameter to extractor.extract(row)
        }
        return constructor!!.callBy(argsMap)
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
            val objectInstance = kClass.objectInstance
            if (objectInstance != null) {
                return RowDeserializer(constructor = null, parameterExtractors = emptyList(), singleton = objectInstance)
            }
            val constructor = kClass.primaryConstructor ?: error("No primary constructor for ${kClass.simpleName}")
            constructor.isAccessible = true
            val argMap = buildTypeArgMap(kType)
            val extractors = constructor.parameters.map { param ->
                val paramName = when {
                    kClass == Pair::class && param.index == 0 -> "_1"
                    kClass == Pair::class && param.index == 1 -> "_2"
                    kClass == Triple::class && param.index == 0 -> "_1"
                    kClass == Triple::class && param.index == 1 -> "_2"
                    kClass == Triple::class && param.index == 2 -> "_3"
                    else -> param.name!!
                }
                // Resolve generic type parameters (e.g. T → String for Box<String>).
                // Pair/Triple use direct index lookup; all other generics use the type arg map.
                val resolvedType = when {
                    kClass == Pair::class || kClass == Triple::class -> kType.arguments[param.index].type!!
                    else -> resolveTypeParam(param.type, argMap)
                }
                ParameterExtractor(param, paramName, resolvedType)
            }
            return RowDeserializer(constructor, extractors)
        }
    }
}
