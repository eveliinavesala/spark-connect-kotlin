package resilience

import classes.SparkTestBase
import kotlinx.datetime.Instant
import kotlinx.serialization.MissingFieldException
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.assertThrows
import spark.kotlin.serialization.schemaFor
import spark.kotlin.serialization.toSerializableDataFrame
import spark.kotlin.serialization.toSerializableKotlinList

/**
 * Validates adapter resilience for SCD (Slowly Changing Dimension) Type 2 table structures.
 *
 * Tests NOT SCD implementation logic (Spark handles that) — they validate that the
 * Kotlin-Spark serialization adapter correctly handles the schema patterns that SCD Type 2
 * introduces: nullable temporal columns, schema evolution when migrating from a simple
 * dimension to a temporal one, backwards compatibility, and pre-flight drift detection.
 *
 * ## SCD Type 2 schema pattern
 *
 * V1 (simple dimension):  `[id, name, tier]`
 * V2 (SCD Type 2):        `[id, name, tier, validFrom?, validTo?, isCurrent?]`
 *
 * ## Data classes
 *
 * Defined here (not in ScdModels.kt) with distinct names ([SimpleDimension], [ScdDimension])
 * to avoid naming conflict with the existing [CustomerV1]/[CustomerV2] in [ScdModels].
 *
 * Run: `./gradlew demoTest`
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScdType2ResilienceTest : SparkTestBase() {
    // ── Data models ───────────────────────────────────────────────────────────

    /** V1: simple dimension — no temporal columns. */
    @Serializable
    data class SimpleDimension(
        val id: Int,
        val name: String,
        val tier: String,
    )

    /**
     * V2: SCD Type 2 dimension — temporal columns added, all nullable.
     * Nullable allows the model to read both V1 data (columns absent → null)
     * and V2 data (columns populated or null for current records).
     */
    @Serializable
    data class ScdDimension(
        val id: Int,
        val name: String,
        val tier: String,
        val validFrom: Instant?,
        val validTo: Instant?,
        val isCurrent: Boolean?,
    )

    /**
     * V2 with explicit Kotlin defaults on temporal fields.
     * Identical schema to [ScdDimension] — differs only in `= null` declarations.
     * Used in Test 5 to show the workaround for the [ScdDimension] gap documented in Test 3.
     */
    @Serializable
    data class ScdDimensionWithDefaults(
        val id: Int,
        val name: String,
        val tier: String,
        val validFrom: Instant? = null,
        val validTo: Instant? = null,
        val isCurrent: Boolean? = null,
    )

    // ── Test fixtures ─────────────────────────────────────────────────────────

    private val v1Data =
        listOf(
            SimpleDimension(1, "Alice", "Gold"),
            SimpleDimension(2, "Bob", "Silver"),
            SimpleDimension(3, "Carol", "Bronze"),
        )

    private val v2DataWithHistory =
        listOf(
            // Expired record: Alice was Gold tier, superseded on 2024-06-15
            ScdDimension(
                1,
                "Alice",
                "Gold",
                validFrom = Instant.parse("2024-01-01T00:00:00Z"),
                validTo = Instant.parse("2024-06-15T00:00:00Z"),
                isCurrent = false,
            ),
            // Current record: Alice is now Silver tier
            ScdDimension(
                1,
                "Alice",
                "Silver",
                validFrom = Instant.parse("2024-06-15T00:00:00Z"),
                validTo = null,
                isCurrent = true,
            ),
            // Current record: Bob, no history
            ScdDimension(
                2,
                "Bob",
                "Bronze",
                validFrom = Instant.parse("2024-01-01T00:00:00Z"),
                validTo = null,
                isCurrent = true,
            ),
        )

    // ── Test 1: V1 data → Spark-side migration → decode as ScdDimension ───────

    /**
     * Simulates an in-flight SCD Type 2 migration: existing V1 data is enriched with
     * temporal columns via Spark `withColumn` before being decoded with the V2 model.
     *
     * Proves: nullable [Instant] columns produced by Spark SQL expressions are decoded
     * correctly, and the original fields are preserved through the migration.
     */
    @Test
    @Order(1)
    fun `v1 data migrated to scd type 2 structure decodes correctly`() {
        val v1Df = v1Data.toSerializableDataFrame(spark)

        // Simulate: ALTER TABLE ADD COLUMNS validFrom TIMESTAMP, validTo TIMESTAMP, isCurrent BOOLEAN
        val scdDf =
            v1Df
                .withColumn("validFrom", current_timestamp())
                .withColumn("validTo", lit(null).cast(DataTypes.TimestampType))
                .withColumn("isCurrent", lit(true))

        val result = scdDf.toSerializableKotlinList<ScdDimension>()

        assertEquals(3, result.size, "All 3 customers should decode after SCD migration")
        result.forEach { customer ->
            assertNotNull(customer.validFrom, "validFrom must be populated by current_timestamp()")
            assertNull(customer.validTo, "validTo must be NULL for current records")
            assertTrue(customer.isCurrent == true, "isCurrent must be true for migrated records")
        }

        val alice = checkNotNull(result.find { it.name == "Alice" }) { "Alice not found" }
        assertEquals(1, alice.id)
        assertEquals("Gold", alice.tier)

        println("[Test 1] V1→SCD migration: ${result.size} customers decoded, temporal columns populated correctly")
    }

    // ── Test 2: Full ScdDimension round-trip with historical records ──────────

    /**
     * Full encode → decode round-trip with a realistic SCD Type 2 dataset:
     * one expired record (validTo set, isCurrent=false) and two current records (validTo=null).
     *
     * Proves: nullable [Instant] handling is correct in both directions —
     * non-null timestamps are preserved exactly, null validTo values survive the round-trip.
     */
    @Test
    @Order(2)
    fun `scd type 2 full round-trip preserves temporal columns and history records`() {
        val df = v2DataWithHistory.toSerializableDataFrame(spark)
        val result = df.toSerializableKotlinList<ScdDimension>()

        assertEquals(3, result.size, "All 3 records including historical should decode")

        // Expired record: Gold Alice — validTo is set, isCurrent=false
        val expired =
            checkNotNull(result.find { it.tier == "Gold" && it.isCurrent == false }) {
                "Expired Gold/Alice record not found"
            }
        assertEquals(1, expired.id)
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), expired.validFrom)
        assertEquals(Instant.parse("2024-06-15T00:00:00Z"), expired.validTo)
        assertFalse(expired.isCurrent!!, "Expired record: isCurrent must be false")

        // Current record: Silver Alice — validTo is null
        val aliceCurrent =
            checkNotNull(result.find { it.name == "Alice" && it.isCurrent == true }) {
                "Current Silver/Alice record not found"
            }
        assertEquals(Instant.parse("2024-06-15T00:00:00Z"), aliceCurrent.validFrom)
        assertNull(aliceCurrent.validTo, "Current record: validTo must be null")

        // Current record: Bob
        val bob = checkNotNull(result.find { it.name == "Bob" }) { "Bob not found" }
        assertEquals(Instant.parse("2024-01-01T00:00:00Z"), bob.validFrom)
        assertNull(bob.validTo)
        assertTrue(bob.isCurrent!!)

        println("[Test 2] SCD Type 2 round-trip: expired validTo and null validTo both preserved correctly")
    }

    // ── Test 3: V1 DataFrame decoded as ScdDimension (backwards compatibility) ─

    /**
     * Proves that `nullable` alone is NOT sufficient for backwards compatibility.
     *
     * [ScdDimension] declares `validFrom`, `validTo`, and `isCurrent` as nullable (`T?`) but
     * without explicit Kotlin default values (`= null`). When V1 data (no temporal columns) is
     * decoded with this model, kotlinx.serialization throws [kotlinx.serialization.MissingFieldException]
     * because the generated deserializer treats fields without a declared default as REQUIRED —
     * even when the type is nullable.
     *
     * **What this proves:** nullability and optionality are distinct concepts in the
     * kotlinx.serialization model. A field typed `T?` is nullable (can hold null at runtime)
     * but is still required during deserialization unless an explicit `= null` default is declared.
     * Deploying a V2 model with nullable-but-required SCD columns against a V1 dataset will
     * crash at decode time, not silently produce nulls.
     *
     * See Test 5 ([ScdDimensionWithDefaults]) for the correct pattern: explicit `= null`
     * defaults make the fields genuinely optional during deserialization.
     */
    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
    @Test
    @Order(3)
    fun `v1 data decoded as scd type 2 model - nullable without default throws MissingFieldException`() {
        val v1Df = v1Data.toSerializableDataFrame(spark)

        // V1 schema has [id, name, tier]; ScdDimension expects [id, name, tier, validFrom?, validTo?, isCurrent?].
        // Nullable fields without explicit = null defaults are REQUIRED by kotlinx.serialization —
        // the deserializer throws rather than substituting null for absent columns.
        val ex =
            assertThrows<MissingFieldException> {
                v1Df.toSerializableKotlinList<ScdDimension>()
            }

        assertTrue(
            ex.message?.contains("validFrom") == true ||
                ex.message?.contains("validTo") == true ||
                ex.message?.contains("isCurrent") == true,
            "MissingFieldException must name at least one of the absent SCD temporal fields — " +
                "proves these nullable fields are treated as required by the deserializer",
        )

        println("[Test 3] EVIDENCE: nullable T? without = null default is required during deserialization")
        println("[Test 3] Exception: ${ex.message}")
        println("[Test 3] Fix: declare fields with explicit = null (see Test 5 — ScdDimensionWithDefaults)")
    }

    // ── Test 4: Pre-flight schema drift detection for SCD migration ───────────

    /**
     * Pre-flight schema comparison surfaces the three missing SCD columns before any row
     * is decoded. The drift report names each field, its expected type, and the trigger.
     *
     * Proves: [SchemaDriftReport] is useful as a migration readiness check — running the
     * comparison before deploying the V2 model confirms exactly which columns need to be
     * added by the migration job.
     */
    @Test
    @Order(4)
    fun `schema drift detection identifies three missing scd type 2 columns`() {
        val v1Df = v1Data.toSerializableDataFrame(spark)

        val expectedSchema = schemaFor(serializer<ScdDimension>()) // V2 model expectation
        val actualSchema = v1Df.schema() // V1 data reality

        val diffs = SchemaDriftReport.compare(expectedSchema, actualSchema)

        assertEquals(3, diffs.size, "Exactly 3 SCD columns should be flagged as missing")

        val missingNames = diffs.map { it.fieldName }.toSet()
        assertTrue("validFrom" in missingNames, "validFrom must be in drift report")
        assertTrue("validTo" in missingNames, "validTo must be in drift report")
        assertTrue("isCurrent" in missingNames, "isCurrent must be in drift report")

        diffs.forEach { diff ->
            assertEquals(
                DriftKind.FIELD_REMOVED,
                diff.kind,
                "'${diff.fieldName}' should be FIELD_REMOVED — present in model, absent from data",
            )
            assertNull(
                diff.actualType,
                "'${diff.fieldName}' should have no actual type (column absent from V1 schema)",
            )
        }

        // Verify inferred Spark types: Instant → TimestampType → "timestamp", Boolean → "boolean"
        assertEquals("timestamp", diffs.find { it.fieldName == "validFrom" }!!.expectedType)
        assertEquals("timestamp", diffs.find { it.fieldName == "validTo" }!!.expectedType)
        assertEquals("boolean", diffs.find { it.fieldName == "isCurrent" }!!.expectedType)

        val report =
            SchemaDriftReport(
                trigger = SchemaDriftReport.triggerFrom(diffs),
                typeName = "ScdDimension",
                expectedSchema = expectedSchema,
                actualSchema = actualSchema,
                diffs = diffs,
            )
        println(report.format())

        // FIELD_REMOVED takes priority in triggerFrom() — all 3 diffs are FIELD_REMOVED
        assertEquals(DriftTrigger.MISSING_FIELD, report.trigger)

        println(
            "[Test 4] Pre-flight drift: 3 SCD columns detected as missing — validFrom(timestamp), validTo(timestamp), isCurrent(boolean)",
        )
    }

    // ── Test 5: Workaround for Test 3 gap — explicit = null defaults ──────────

    /**
     * Workaround for the gap documented in Test 3.
     *
     * [ScdDimensionWithDefaults] is structurally identical to [ScdDimension] — same fields,
     * same types — but its temporal fields declare explicit `= null` Kotlin defaults.
     * This signals to the `kotlinx.serialization` generated deserializer that unvisited
     * fields are optional, preventing [kotlinx.serialization.MissingFieldException].
     *
     * The decoder skips absent columns exactly as before. The difference is that the
     * serialization framework now accepts the omission rather than throwing.
     */
    @Test
    @Order(5)
    fun `scd type 2 with explicit null defaults - absent v1 columns decode as null`() {
        val v1Df = v1Data.toSerializableDataFrame(spark)

        val result = v1Df.toSerializableKotlinList<ScdDimensionWithDefaults>()

        assertEquals(3, result.size, "All V1 records should decode when temporal fields have explicit defaults")
        result.forEach { customer ->
            assertNull(customer.validFrom, "Absent validFrom should be null")
            assertNull(customer.validTo, "Absent validTo should be null")
            assertNull(customer.isCurrent, "Absent isCurrent should be null")
        }

        val alice = checkNotNull(result.find { it.name == "Alice" }) { "Alice not found" }
        assertEquals(1, alice.id)
        assertEquals("Gold", alice.tier)

        println("[Test 5] Workaround confirmed: ScdDimensionWithDefaults reads V1 data — temporal columns null, V1 fields intact")
    }
}
