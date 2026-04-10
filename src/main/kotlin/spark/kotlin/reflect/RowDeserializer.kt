package spark.kotlin.reflect

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KType
import kotlin.reflect.full.*
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmErasure

/**
 * Converts a Spark [Row] to a Kotlin object by invoking the target type's primary constructor
 * via reflection, with column values mapped to constructor parameters by name.
 *
 * Two decode paths are provided:
 * - **Per-row** ([deserialize(Row)]): builds the column-index map on first call by scanning
 *   `row.schema()`. Suitable for single-row or sealed-class decoding where column sets differ.
 * - **Batch** ([deserialize(Row, IntArray)]): accepts a pre-built index map produced by
 *   [buildColumnIndexMap]. The schema scan is performed once per batch, amortising its cost
 *   across all rows. Prefer this path when decoding large [org.apache.spark.sql.Dataset] batches.
 *
 * [Pair] and [Triple] fields are mapped to Scala tuple column names (`_1`, `_2`, `_3`).
 * Generic type parameters are resolved from the concrete [kotlin.reflect.KType] to avoid
 * type-argument loss from JVM erasure.
 *
 * Companion factory [RowDeserializer.create] constructs extractors from the primary constructor
 * parameters; Kotlin object singletons bypass field extraction entirely.
 */
internal class RowDeserializer<T : Any> private constructor(
    private val constructor: kotlin.reflect.KFunction<T>?,
    private val parameterExtractors: List<ParameterExtractor>,
    private val singleton: T? = null
) {
    /** Deserializes [row] using per-row schema scanning. For batch decoding prefer [deserialize(Row, IntArray)]. */
    fun deserialize(row: Row): T {
        if (singleton != null) return singleton
        val argsMap = parameterExtractors.associate { extractor ->
            extractor.parameter to extractor.extract(row)
        }
        return constructor!!.callBy(argsMap)
    }

    /**
     * Deserialize using pre-built column indices (batch-decode path).
     *
     * [columnIndices] maps extractor position → column index in the Row.
     * Use [buildColumnIndexMap] to produce this array once per DataFrame schema
     * and pass it to every row in the batch. Avoids per-row schema field scans.
     */
    fun deserialize(row: Row, columnIndices: IntArray): T {
        if (singleton != null) return singleton
        val argsMap = parameterExtractors.mapIndexed { i, extractor ->
            extractor.parameter to extractor.extractByIndex(row, columnIndices[i])
        }.toMap()
        return constructor!!.callBy(argsMap)
    }

    /**
     * Build a column-index array from a DataFrame [schema].
     * Each entry corresponds to the extractor at the same position: -1 means absent.
     * Call this once per batch; pass the result to [deserialize].
     */
    internal fun buildColumnIndexMap(schema: StructType): IntArray =
        IntArray(parameterExtractors.size) { i ->
            try { schema.fieldIndex(parameterExtractors[i].paramName) } catch (_: IllegalArgumentException) { -1 }
        }

    private class ParameterExtractor(val parameter: KParameter, val paramName: String, val paramType: KType) {
        fun extract(row: Row): Any? {
            val rawValue: Any? = row.getAs(paramName)
            if (rawValue == null && !parameter.type.isMarkedNullable) {
                throw IllegalArgumentException("Null value received for non-nullable parameter '$paramName'")
            }
            return convertSparkValueToKotlin(rawValue, paramType)
        }

        /** Index-based variant used by the batch-decode path. */
        fun extractByIndex(row: Row, columnIndex: Int): Any? {
            if (columnIndex < 0) {
                if (!parameter.type.isMarkedNullable) {
                    throw IllegalArgumentException("Column for non-nullable parameter '$paramName' is absent from the schema")
                }
                return null
            }
            val rawValue: Any? = row.get(columnIndex)
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
