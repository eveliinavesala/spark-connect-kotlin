package spark.kotlin.reflect

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure

/**
 * Converts a Kotlin object to a Spark [Row] using pre-built [FieldSerializer] extractors.
 *
 * An instance is bound to a specific [StructType] at construction time. Each field in the schema
 * has a corresponding [FieldSerializer] that knows how to extract and convert the matching property.
 *
 * Special cases handled during field extraction:
 * - `_type` field — filled with `obj::class.simpleName` for sealed union schemas
 * - [Pair] / [Triple] — properties are mapped to `_1`, `_2`, `_3` by index rather than by name
 * - Generic type arguments — preserved via a resolved [kotlin.reflect.KType] so that nested
 *   generic classes (e.g. `Box<String>`) are not reduced to `Box<*>` by JVM erasure
 * - Sealed union schema fields absent on the runtime subtype — resolved dynamically via
 *   [kotlin.reflect.KClass.memberProperties] and return `null` when absent
 *
 * Instances are obtained through [ReflectionCache.getSerializer]; direct construction is via
 * the [companion object][RowSerializer.Companion].
 */
internal class RowSerializer private constructor(
    private val schema: StructType,
    private val fieldSerializers: List<FieldSerializer>,
) {
    /** Extracts each field and returns a [GenericRowWithSchema] bound to [schema]. */
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
        val declaredType: KType? = null,
    ) {
        fun extract(obj: Any): Any? =
            when {
                fieldName == "_type" -> {
                    obj::class.simpleName
                }

                obj is Pair<*, *> || obj is Triple<*, *, *> -> {
                    extractTupleField(obj)
                }

                property != null -> {
                    property.isAccessible = true
                    convertKotlinValueToSpark(property.call(obj), declaredType)
                }

                else -> {
                    extractSealedField(obj)
                }
            }

        private fun extractTupleField(obj: Any): Any? =
            when {
                obj is Pair<*, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Pair<*, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_1" -> convertKotlinValueToSpark(obj.first)
                obj is Triple<*, *, *> && fieldName == "_2" -> convertKotlinValueToSpark(obj.second)
                obj is Triple<*, *, *> && fieldName == "_3" -> convertKotlinValueToSpark(obj.third)
                else -> null
            }

        private fun extractSealedField(obj: Any): Any? {
            // Sealed class union schema: a field may only exist on some subclasses.
            // Look up the property on the runtime type and return null if absent.
            val runtimeProp =
                obj::class
                    .memberProperties
                    .find { it.name == fieldName }
                    ?.also { it.isAccessible = true }
                    ?: return null
            // Resolve the property's declared type against the runtime subtype's type arguments
            // so that generic fields (e.g. List<Address>, Box<String>) are not erased to List<*>.
            // For non-generic subtypes buildTypeArgMap returns an empty map and resolveTypeParam
            // returns the type unchanged.
            val runtimeType = resolveTypeParam(runtimeProp.returnType, buildTypeArgMap(obj::class.createType()))
            return convertKotlinValueToSpark(runtimeProp.call(obj), runtimeType)
        }
    }

    companion object {
        fun create(
            kType: KType,
            providedSchema: StructType? = null,
        ): RowSerializer {
            val kClass = kType.jvmErasure
            val schema = providedSchema ?: ReflectionCache.getSchema(kType)
            val argMap = buildTypeArgMap(kType)
            val fieldSerializers =
                schema.fields().map { field ->
                    // Single lookup — used for both the property reference and declared type resolution
                    val memberProp =
                        if (kClass == Pair::class || kClass == Triple::class) {
                            null
                        } else {
                            kClass.memberProperties.find { it.name == field.name() }
                        }
                    val prop =
                        memberProp
                            ?: if (field.name() == "_type" && kClass.isSealed) {
                                null
                            } else if (kClass.isSealed) {
                                null // union schema field absent on this subclass — resolved at runtime
                            } else if (kClass == Pair::class || kClass == Triple::class) {
                                null // handled by FieldSerializer.extract
                            } else {
                                error("Property '${field.name()}' not found in ${kClass.simpleName}")
                            }
                    // Resolve generic type args so nested Box<String> isn't lost to erasure as Box<*>
                    val declaredType = memberProp?.returnType?.let { resolveTypeParam(it, argMap) }
                    FieldSerializer(field.name(), prop, declaredType)
                }
            return RowSerializer(schema, fieldSerializers)
        }
    }
}
