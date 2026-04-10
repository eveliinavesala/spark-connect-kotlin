package resilience

import classes.SparkTestBase
import kotlinx.serialization.serializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.serialization.schemaFor
import spark.kotlin.serialization.toSerializableDataFrame

/**
 * Demonstrates Slowly Changing Dimension (SCD) detection and the BackendRouter failover.
 *
 * Three SCD scenarios:
 *   1. Field removed from data — model expects a non-nullable field that no longer exists
 *   2. Field added to data    — source gained a new column the model doesn't know about
 *   3. Pre-flight comparison  — schema drift detected before any row is decoded
 *
 * The "Immediate Inspection" output (the drift report printed to stdout) is the central
 * thesis claim: developers see a structured diff, not an opaque stack trace.
 */
class ScdFailoverTest : SparkTestBase() {

    // ── Sample data ───────────────────────────────────────────────────────────

    private val v1Data = listOf(
        CustomerV1("c1", "Alice", "Gold"),
        CustomerV1("c2", "Bob",   "Silver")
    )

    private val v2Data = listOf(
        CustomerV2("c1", "Alice", "Gold",   92),
        CustomerV2("c2", "Bob",   "Silver", 74)
    )

    // ── Scenario 1: SCD — field removed from data ─────────────────────────────

    @Test
    fun `SCD field removed - pre-flight schema comparison surfaces drift before decoding`() {
        // Simulate: encode V2 data, then drop "score" to produce V1-shape DataFrame
        val v2Df = v2Data.toSerializableDataFrame(spark)
        val v1ShapeDf = v2Df.drop("score")  // SCD: score column removed from storage

        // Pre-flight: compare what V2 class expects vs what the data actually has
        val expectedSchema = schemaFor(serializer<CustomerV2>())
        val actualSchema   = v1ShapeDf.schema()
        val diffs          = SchemaDriftReport.compare(expectedSchema, actualSchema)

        // Assert drift is detected at schema level — before a single row is decoded
        assertTrue(diffs.isNotEmpty(), "Schema comparison should surface the missing 'score' field")
        val scoreDiff = diffs.find { it.fieldName == "score" }
        assertNotNull(scoreDiff, "Drift report should name the removed field")
        assertEquals(DriftKind.FIELD_REMOVED, scoreDiff!!.kind)
        assertEquals("int", scoreDiff.expectedType)  // simpleString() is lowercase
        assertNull(scoreDiff.actualType)

        // Print the Immediate Inspection report
        val report = SchemaDriftReport(
            trigger        = SchemaDriftReport.triggerFrom(diffs),
            typeName       = "CustomerV2",
            expectedSchema = expectedSchema,
            actualSchema   = actualSchema,
            diffs          = diffs
        )
        println(report.format())

        assertEquals(DriftTrigger.MISSING_FIELD, report.trigger)
    }

    @Test
    fun `SCD field removed - router returns BothFailed for non-nullable missing field`() {
        // V2-shape data encoded, then score column dropped — simulates SCD field removal
        val v2Df      = v2Data.toSerializableDataFrame(spark)
        val v1ShapeDf = v2Df.drop("score")

        // The router detects drift pre-flight, then attempts reflection fallback.
        // Reflection ALSO fails: the Row doesn't have "score" and CustomerV2.score is Int (non-null).
        // This proves the Kotlin type system enforces the contract — silent nulls are not injected.
        val result = BackendRouter.decode(v1ShapeDf, serializer<CustomerV2>())

        assertTrue(result is RouterResult.BothFailed<*>, "Expected BothFailed but got ${result::class.simpleName}")
        val failed = result as RouterResult.BothFailed<CustomerV2>
        assertEquals(DriftTrigger.MISSING_FIELD, failed.report.trigger)
        assertTrue(failed.report.diffs.any { it.fieldName == "score" && it.kind == DriftKind.FIELD_REMOVED })

        println("""
            |── Developer action ──────────────────────────────────────────────────────────
            |  BothFailed confirms that the missing 'score' field cannot be recovered without
            |  either:
            |    a) Migrating the data  — add 'score' column with a sensible default
            |    b) Relaxing the model  — change CustomerV2 to CustomerV3 (score: Int?)
            |──────────────────────────────────────────────────────────────────────────────
        """.trimMargin())
    }

    @Test
    fun `SCD field removed - nullable migration (V3) succeeds with reflection fallback`() {
        // V2-shape DataFrame, drop score → V1-shape. Try to decode as V3 (nullable score).
        // Both backends still fail because the Row doesn't have the "score" column at all.
        // The correct migration is to also fill the missing column before decoding.
        val v2Df      = v2Data.toSerializableDataFrame(spark)
        val v1ShapeDf = v2Df.drop("score")
        val migratedDf = v1ShapeDf.withColumn("score",
            org.apache.spark.sql.functions.lit(null).cast(org.apache.spark.sql.types.DataTypes.IntegerType))

        // Now V3 (nullable score) can decode successfully via both backends
        val result = BackendRouter.decode(migratedDf, serializer<CustomerV3>())

        assertTrue(result is RouterResult.SerializationSuccess<*>, "Expected SerializationSuccess but got ${result::class.simpleName}")
        val customers = (result as RouterResult.SerializationSuccess<CustomerV3>).data
        assertEquals(2, customers.size)
        assertNull(customers[0].score, "Migrated score column should deserialize as null")

        println("Migration pattern: withColumn(\"score\", lit(null).cast(IntegerType)) → V3 decoded successfully")
    }

    // ── Scenario 2: SCD — field added to data ────────────────────────────────

    @Test
    fun `SCD field added to data - pre-flight shows SCD_ADDITION drift`() {
        // V1 model, V2-shape DataFrame (has extra "score" column)
        val v2Df = v2Data.toSerializableDataFrame(spark)

        val expectedSchema = schemaFor(serializer<CustomerV1>())  // V1 model
        val actualSchema   = v2Df.schema()                        // V2-shape data
        val diffs          = SchemaDriftReport.compare(expectedSchema, actualSchema)

        assertTrue(diffs.isNotEmpty(), "New 'score' column should be flagged as drift")
        val scoreDiff = diffs.find { it.fieldName == "score" }
        assertNotNull(scoreDiff)
        assertEquals(DriftKind.FIELD_ADDED, scoreDiff!!.kind)

        val report = SchemaDriftReport(
            trigger        = SchemaDriftReport.triggerFrom(diffs),
            typeName       = "CustomerV1",
            expectedSchema = expectedSchema,
            actualSchema   = actualSchema,
            diffs          = diffs
        )
        println(report.format())
        assertEquals(DriftTrigger.SCD_ADDITION, report.trigger)
    }

    @Test
    fun `SCD field added to data - decode succeeds but new field is silently ignored`() {
        // Serialization backend: ignores unknown columns — V1 class decoded from V2 DataFrame
        // This is the "invisible schema" risk: data is present but not captured in the model
        val v2Df = v2Data.toSerializableDataFrame(spark)

        val result = BackendRouter.decode(v2Df, serializer<CustomerV1>())

        // SCD_ADDITION drift is reported but fallback is not needed — serialization handles it
        // (It silently drops the "score" column; the report surfaces this for governance)
        when (result) {
            is RouterResult.ReflectionFallback -> {
                println("ReflectionFallback triggered — report generated for governance:")
                println(result.report.format())
                assertTrue(result.report.diffs.any { it.kind == DriftKind.FIELD_ADDED })
            }
            is RouterResult.SerializationSuccess -> {
                // Some backends may succeed silently — pre-flight comparison is the safeguard
                println("Note: serialization succeeded silently. Pre-flight comparison found drift:")
                val diffs = SchemaDriftReport.compare(schemaFor(serializer<CustomerV1>()), v2Df.schema())
                assertTrue(diffs.any { it.kind == DriftKind.FIELD_ADDED },
                    "Pre-flight must surface the new 'score' column even when decode succeeds")
            }
            is RouterResult.BothFailed -> fail("Both backends should not fail when data has extra columns")
        }
    }

    // ── Scenario 3: type-changed field ────────────────────────────────────────

    @Test
    fun `SCD type changed - pre-flight flags TYPE_CHANGED drift`() {
        // Simulate: encode V1 data, cast "tier" column to Integer (wrong type migration)
        val v1Df      = v1Data.toDataFrame(spark)
        val wrongTypeDf = v1Df.withColumn("tier",
            org.apache.spark.sql.functions.lit(1))  // tier is now Int, not String

        val expectedSchema = schemaFor(serializer<CustomerV1>())
        val actualSchema   = wrongTypeDf.schema()
        val diffs          = SchemaDriftReport.compare(expectedSchema, actualSchema)

        assertTrue(diffs.any { it.fieldName == "tier" && it.kind == DriftKind.TYPE_CHANGED },
            "Type change on 'tier' must be surfaced")

        val report = SchemaDriftReport(
            trigger        = SchemaDriftReport.triggerFrom(diffs),
            typeName       = "CustomerV1",
            expectedSchema = expectedSchema,
            actualSchema   = actualSchema,
            diffs          = diffs
        )
        println(report.format())
        assertEquals(DriftTrigger.TYPE_MISMATCH, report.trigger)
    }
}
