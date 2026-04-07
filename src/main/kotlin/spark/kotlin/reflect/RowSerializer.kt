package spark.kotlin.reflect

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure

internal class RowSerializer private constructor(
    private val schema: StructType,
    private val fieldSerializers: List<FieldSerializer>
) {
    fun serialize(obj: Any): Row {
        val values = fieldSerializers.map { it.extract(obj) }.toTypedArray()
        return GenericRowWithSchema(values, schema)
    }

    private class FieldSerializer(
        val fieldName: String,
        val property: KProperty1<out Any, *>?,
        // Declared KType with resolved type arguments (e.g. Box<String>, not Box<*>).
        // Used to preserve generic type context when serializing nested generic data classes,
        // where the runtime value alone would lose the type argument to JVM erasure.
        val declaredType: KType? = null
    ) {
        fun extract(obj: Any): Any? {
            return when {
                fieldName == "_type" -> obj::class.simpleName
                // Special handling for Pair/Triple mapping _1 -> first, etc.
                obj is Pair<*, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Pair<*, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Triple<*, *, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_3" -> convertKotlinValueToSpark(obj.third)
                property != null -> {
                    property.isAccessible = true
                    convertKotlinValueToSpark(property.call(obj), declaredType)
                }
                else -> {
                    // Sealed class union schema: a field may only exist on some subclasses.
                    // Look up the property on the runtime type and return null if absent.
                    val runtimeProp = obj::class.memberProperties.find { it.name == fieldName }
                        ?.also { it.isAccessible = true }
                    convertKotlinValueToSpark(runtimeProp?.call(obj))
                }
            }
        }
    }

    companion object {
        fun create(kType: KType, providedSchema: StructType? = null): RowSerializer {
            val kClass = kType.jvmErasure
            val schema = providedSchema ?: ReflectionCache.getSchema(kType)
            val argMap = buildTypeArgMap(kType)
            val fieldSerializers = schema.fields().map { field ->
                // Single lookup — used for both the property reference and declared type resolution
                val memberProp = if (kClass == Pair::class || kClass == Triple::class) null
                                 else kClass.memberProperties.find { it.name == field.name() }
                val prop = memberProp
                    ?: if (field.name() == "_type" && kClass.isSealed) null
                       else if (kClass.isSealed) null // union schema field absent on this subclass — resolved at runtime
                       else error("Property '${field.name()}' not found in ${kClass.simpleName}")
                // Resolve generic type args so nested Box<String> isn't lost to erasure as Box<*>
                val declaredType = memberProp?.returnType?.let { resolveTypeParam(it, argMap) }
                FieldSerializer(field.name(), prop, declaredType)
            }
            return RowSerializer(schema, fieldSerializers)
        }
    }
}
