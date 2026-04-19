package resilience

import kotlinx.serialization.KSerializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import spark.kotlin.serialization.schemaFor
import spark.kotlin.serialization.toSerializableDataFrame
import spark.kotlin.serialization.toSerializableKotlinList

// ── Result types ──────────────────────────────────────────────────────────────

sealed class RouterResult<T> {
    /** Serialization backend succeeded — fast path, no drift. */
    data class SerializationSuccess<T>(
        val data: List<T>,
    ) : RouterResult<T>()

    /** Serialization backend detected drift; reflection backend recovered the data. */
    data class ReflectionFallback<T>(
        val data: List<T>,
        val report: SchemaDriftReport,
    ) : RouterResult<T>()

    /**
     * Drift detected and both backends failed to decode (e.g. non-nullable field absent from data).
     * The [report] shows exactly which fields caused the failure.
     * Developer action required: align the Kotlin class with the actual schema before proceeding.
     */
    data class BothFailed<T>(
        val report: SchemaDriftReport,
        val reflectionCause: Exception,
    ) : RouterResult<T>()
}

// ── Router ────────────────────────────────────────────────────────────────────

/**
 * User-space dual-backend coordinator. Wraps the library's public API surface:
 *   spark.kotlin.reflect.*  — reflection backend
 *   spark.kotlin.serialization.* — serialization backend
 *
 * Data engineers use this to get production-speed serialization with transparent
 * fallback to the reflection backend when schema drift is detected.
 * No library-core changes are needed to use or extend this pattern.
 */
object BackendRouter {
    // ── Encode (List<T> → DataFrame) ──────────────────────────────────────────

    /**
     * Encode a list to a DataFrame, preferring the serialization backend.
     *
     * When [serializer] is null (type-gap: BigDecimal, Set<T>, java.time.*, etc.),
     * routes directly to the reflection backend with no drift report — the gap is
     * an acknowledged design decision, not a runtime failure.
     *
     * When [serializer] is provided but serialization fails, falls back to reflection
     * and returns a [SchemaDriftReport] describing what diverged.
     *
     * @return DataFrame + optional drift report (null = no drift, serialization succeeded)
     */
    inline fun <reified T : Any> encode(
        data: List<T>,
        spark: SparkSession,
        serializer: KSerializer<T>?,
    ): Pair<Dataset<Row>, SchemaDriftReport?> {
        if (serializer == null) {
            // Type-gap path: serialization not applicable — reflection is the only option
            return data.toDataFrame(spark) to null
        }
        return try {
            data.toSerializableDataFrame(spark) to null
        } catch (e: Exception) {
            val serializationSchema = schemaFor(serializer)
            val reflectionDf = data.toDataFrame(spark)
            val actualSchema = reflectionDf.schema()
            val diffs = SchemaDriftReport.compare(serializationSchema, actualSchema)
            val report =
                SchemaDriftReport(
                    trigger = SchemaDriftReport.triggerFrom(diffs),
                    typeName = T::class.simpleName ?: "Unknown",
                    expectedSchema = serializationSchema,
                    actualSchema = actualSchema,
                    diffs = diffs,
                    cause = e,
                )
            println(report.format())
            reflectionDf to report
        }
    }

    // ── Decode (DataFrame → List<T>) ──────────────────────────────────────────

    /**
     * Decode a DataFrame to a typed list, preferring the serialization backend.
     *
     * Schema comparison is performed BEFORE attempting to decode (proactive detection).
     * If drift is found, the report is generated immediately and the reflection backend
     * is attempted as fallback.
     *
     * Returns:
     * - [RouterResult.SerializationSuccess] — fast path, no drift
     * - [RouterResult.ReflectionFallback]   — drift detected, reflection recovered
     * - [RouterResult.BothFailed]           — drift detected, both backends failed
     *   (e.g. non-nullable field absent from data — requires Kotlin class or data migration)
     */
    inline fun <reified T : Any> decode(
        df: Dataset<Row>,
        serializer: KSerializer<T>,
    ): RouterResult<T> {
        val actualSchema = df.schema()
        val expectedSchema = schemaFor(serializer)
        val diffs = SchemaDriftReport.compare(expectedSchema, actualSchema)

        if (diffs.isEmpty()) {
            // No drift — use fast serialization path
            return RouterResult.SerializationSuccess(df.toSerializableKotlinList<T>())
        }

        // Drift detected — generate Immediate Inspection report before touching any row
        val report =
            SchemaDriftReport(
                trigger = SchemaDriftReport.triggerFrom(diffs),
                typeName = T::class.simpleName ?: "Unknown",
                expectedSchema = expectedSchema,
                actualSchema = actualSchema,
                diffs = diffs,
            )
        println(report.format())

        // Attempt reflection fallback
        return try {
            RouterResult.ReflectionFallback(df.toKotlinList<T>(), report)
        } catch (e: Exception) {
            RouterResult.BothFailed(report, e)
        }
    }
}
