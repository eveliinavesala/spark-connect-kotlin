package resilience

import classes.SparkTestBase
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.MissingFieldException
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
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
 * Covers gaps left by [ColumnarFormatColumnOrderingTest], [ColumnarFormatPositionalDecoderTest],
 * and [catalog.SchemaGovernanceTest]:
 *
 *   - [SchemaDriftReport.compare] is only ever exercised for [DriftKind.FIELD_REMOVED] elsewhere
 *     ([ScdType2ResilienceTest] test 4). [DriftKind.TYPE_CHANGED] and [DriftKind.FIELD_ADDED],
 *     and their corresponding [DriftTrigger] values (TYPE_MISMATCH, SCD_ADDITION), are never
 *     asserted anywhere.
 *   - Column-name **case sensitivity** is untested for both [SchemaDriftReport.compare] and the
 *     name-based decoder ([spark.kotlin.serialization.decoders.SparkRowDecoder]).
 *
 * ## Why case sensitivity matters here specifically
 *
 * [SchemaDriftReport.compare] keys its comparison map by exact field name
 * (`fields().associate { it.name() to ... }`) — a plain Kotlin `Map<String, String>`. "userId"
 * and "UserId" are different keys, full stop, regardless of Spark's `spark.sql.caseSensitive`
 * setting (which only affects SQL analysis, not this comparison).
 *
 * The name-based decoder resolves columns via `StructType.fieldIndex(name)`, which is backed by
 * `fieldNames.zipWithIndex.toMap` — also a case-sensitive Scala `Map`, independent of
 * `spark.sql.caseSensitive`. A descriptor field "userId" will NOT resolve against a DataFrame
 * column "UserId": `fieldIndex` throws `IllegalArgumentException`, which
 * [spark.kotlin.serialization.decoders.SparkRowDecoder.buildColumnMap] catches and converts to
 * column index `-1` — i.e. "this column does not exist as far as the decoder is concerned".
 *
 * Tests 5 and 6 below demonstrate the two different outcomes this produces depending on whether
 * the mismatched field is required or has an explicit `= null` default — mirroring the
 * required-vs-optional distinction [ScdType2ResilienceTest] established for genuinely absent
 * columns.
 *
 * Run: `./gradlew demoTest`   (no Unity Catalog required — extends [SparkTestBase])
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaDriftEdgeCasesTest : SparkTestBase() {
    // ── Data models for case-sensitivity decode tests ─────────────────────────

    @Serializable
    data class CaseSensitiveRecord(
        val userId: String,
        val sessionId: String,
    )

    /**
     * Structurally identical to [CaseSensitiveRecord] except `userId` is nullable with an
     * explicit `= null` default — following the [ScdType2ResilienceTest] Test 5 pattern for
     * making a field genuinely optional during deserialization (not just nullable).
     */
    @Serializable
    data class CaseSensitiveRecordNullable(
        val sessionId: String,
        val userId: String? = null,
    )

    // ── Test 1: TYPE_CHANGED drift ─────────────────────────────────────────────

    /**
     * When the same field name appears in both schemas but with different Spark types,
     * [SchemaDriftReport.compare] must report [DriftKind.TYPE_CHANGED] (not FIELD_REMOVED or
     * FIELD_ADDED — the field IS present on both sides, just with a different type), and
     * [SchemaDriftReport.triggerFrom] must classify this as [DriftTrigger.TYPE_MISMATCH].
     */
    @Test
    @Order(1)
    fun `type changed - compare detects retyped field and classifies as TYPE_MISMATCH`() {
        val expected =
            StructType(
                arrayOf(
                    DataTypes.createStructField("price", DataTypes.DoubleType, false),
                ),
            )
        val actual =
            StructType(
                arrayOf(
                    DataTypes.createStructField("price", DataTypes.StringType, false),
                ),
            )

        val diffs = SchemaDriftReport.compare(expected, actual)

        assertEquals(1, diffs.size, "Exactly one field differs")
        val diff = diffs.single()
        assertEquals("price", diff.fieldName)
        assertEquals(DriftKind.TYPE_CHANGED, diff.kind)
        assertEquals("double", diff.expectedType)
        assertEquals("string", diff.actualType)

        assertEquals(
            DriftTrigger.TYPE_MISMATCH,
            SchemaDriftReport.triggerFrom(diffs),
            "A lone TYPE_CHANGED diff must trigger TYPE_MISMATCH",
        )

        println("[Test 1] TYPE_CHANGED: 'price' double -> string, trigger=TYPE_MISMATCH")
    }

    // ── Test 2: FIELD_ADDED drift ──────────────────────────────────────────────

    /**
     * When the actual schema has a column the Kotlin model doesn't know about,
     * [SchemaDriftReport.compare] must report [DriftKind.FIELD_ADDED] with `expectedType == null`,
     * and [SchemaDriftReport.triggerFrom] must classify a diff set containing only additions as
     * [DriftTrigger.SCD_ADDITION] — the "new column appeared in the data" case.
     */
    @Test
    @Order(2)
    fun `field added - compare detects new column and classifies as SCD_ADDITION`() {
        val expected =
            StructType(
                arrayOf(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                ),
            )
        val actual =
            StructType(
                arrayOf(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("region", DataTypes.StringType, true),
                ),
            )

        val diffs = SchemaDriftReport.compare(expected, actual)

        assertEquals(1, diffs.size, "Exactly one extra field")
        val diff = diffs.single()
        assertEquals("region", diff.fieldName)
        assertEquals(DriftKind.FIELD_ADDED, diff.kind)
        assertNull(diff.expectedType, "FIELD_ADDED diffs have no expected type — the model doesn't know this field")
        assertEquals("string", diff.actualType)

        assertEquals(
            DriftTrigger.SCD_ADDITION,
            SchemaDriftReport.triggerFrom(diffs),
            "A diff set containing only FIELD_ADDED must trigger SCD_ADDITION",
        )

        println("[Test 2] FIELD_ADDED: 'region' (string) present in data only, trigger=SCD_ADDITION")
    }

    // ── Test 3: Mixed drift — all three kinds at once, with priority ──────────

    /**
     * A schema can drift in more than one way simultaneously: one field removed, one retyped,
     * one added. [SchemaDriftReport.compare] must report all three as separate [FieldDiff]
     * entries with the correct [DriftKind] each — not collapse them, and not stop after the
     * first.
     *
     * [SchemaDriftReport.triggerFrom] has a defined priority order (FIELD_REMOVED >
     * TYPE_CHANGED > FIELD_ADDED > UNKNOWN). With all three kinds present, the trigger must be
     * [DriftTrigger.MISSING_FIELD] — the highest-priority case — even though TYPE_CHANGED and
     * FIELD_ADDED diffs are also present in the same report.
     */
    @Test
    @Order(3)
    fun `mixed drift - removed, retyped, and added fields all detected with correct trigger priority`() {
        // expected: [id: Int, name: String, price: Double]
        val expected =
            StructType(
                arrayOf(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("price", DataTypes.DoubleType, false),
                ),
            )
        // actual: [id: Int, name: Int (retyped), region: String (added)] — "price" removed
        val actual =
            StructType(
                arrayOf(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.IntegerType, false),
                    DataTypes.createStructField("region", DataTypes.StringType, true),
                ),
            )

        val diffs = SchemaDriftReport.compare(expected, actual)

        assertEquals(3, diffs.size, "id matches; name, price, region all differ")

        val byField = diffs.associateBy { it.fieldName }
        assertEquals(DriftKind.TYPE_CHANGED, byField["name"]?.kind, "'name': string -> int is a type change")
        assertEquals(DriftKind.FIELD_REMOVED, byField["price"]?.kind, "'price' is absent from actual schema")
        assertEquals(DriftKind.FIELD_ADDED, byField["region"]?.kind, "'region' is new in actual schema")

        assertEquals(
            DriftTrigger.MISSING_FIELD,
            SchemaDriftReport.triggerFrom(diffs),
            "FIELD_REMOVED ('price') must take priority over TYPE_CHANGED and FIELD_ADDED",
        )

        println("[Test 3] Mixed drift: name=TYPE_CHANGED, price=FIELD_REMOVED, region=FIELD_ADDED, trigger=MISSING_FIELD")
    }

    // ── Test 4: Case sensitivity in SchemaDriftReport.compare ──────────────────

    /**
     * "userId" (expected) and "UserId" (actual) represent the same logical column in any
     * reasonable data model, but [SchemaDriftReport.compare] keys its lookup by exact field
     * name. The two are treated as completely unrelated fields: "userId" is reported as
     * [DriftKind.FIELD_REMOVED] (not found in actual) AND "UserId" is reported as
     * [DriftKind.FIELD_ADDED] (not found in expected) — two diffs for what is, in practice,
     * one renamed-by-casing column.
     *
     * This is documented behavior, not necessarily a bug — but it means a pre-flight check
     * using `compare()` WILL flag a pure casing difference as drift, with [DriftTrigger]
     * resolving to MISSING_FIELD (FIELD_REMOVED takes priority).
     */
    @Test
    @Order(4)
    fun `case sensitivity - compare treats differently-cased field names as two unrelated fields`() {
        val expected =
            StructType(
                arrayOf(
                    DataTypes.createStructField("userId", DataTypes.StringType, false),
                ),
            )
        val actual =
            StructType(
                arrayOf(
                    DataTypes.createStructField("UserId", DataTypes.StringType, false),
                ),
            )

        val diffs = SchemaDriftReport.compare(expected, actual)

        assertEquals(
            2,
            diffs.size,
            "'userId' and 'UserId' are treated as two distinct fields, not one renamed field",
        )

        val byField = diffs.associateBy { it.fieldName }
        assertEquals(DriftKind.FIELD_REMOVED, byField["userId"]?.kind, "'userId' (expected) not found in actual")
        assertEquals(DriftKind.FIELD_ADDED, byField["UserId"]?.kind, "'UserId' (actual) not found in expected")

        assertEquals(
            DriftTrigger.MISSING_FIELD,
            SchemaDriftReport.triggerFrom(diffs),
            "FIELD_REMOVED ('userId') takes priority even though the 'difference' is only casing",
        )

        println("[Test 4] Case sensitivity: 'userId' vs 'UserId' reported as FIELD_REMOVED + FIELD_ADDED, not a match")
    }

    // ── Test 5: Case sensitivity - decoder, required field, no default ────────

    /**
     * [CaseSensitiveRecord.userId] is non-nullable with no default. When the DataFrame column
     * is named "UserId" instead of "userId", `StructType.fieldIndex("userId")` throws
     * `IllegalArgumentException` (case-sensitive exact-name lookup), which
     * [spark.kotlin.serialization.decoders.SparkRowDecoder.buildColumnMap] converts to column
     * index -1. `decodeElementIndex` then never visits the "userId" descriptor element, and
     * kotlinx.serialization's generated deserializer throws [MissingFieldException] because
     * "userId" has no default value — the SAME failure mode [ScdType2ResilienceTest] Test 3
     * demonstrates for a genuinely ABSENT column, even though here the data IS present, just
     * under a different-cased name.
     */
    @OptIn(ExperimentalSerializationApi::class)
    @Test
    @Order(5)
    fun `case sensitivity - decoder throws MissingFieldException when required field's column differs only by case`() {
        val df = listOf(CaseSensitiveRecord(userId = "user_1", sessionId = "session_1")).toSerializableDataFrame(spark)

        // Rename "userId" -> "UserId" — simulates an external source using different casing
        // conventions for the same logical column.
        val renamed = df.withColumnRenamed("userId", "UserId")

        assertTrue(
            renamed.schema().fieldNames().contains("UserId"),
            "Renamed DataFrame must have 'UserId', not 'userId'",
        )
        assertTrue(
            !renamed.schema().fieldNames().contains("userId"),
            "Renamed DataFrame must no longer have lowercase 'userId'",
        )

        val ex =
            assertThrows<MissingFieldException> {
                renamed.toSerializableKotlinList<CaseSensitiveRecord>()
            }

        assertTrue(
            ex.message?.contains("userId") == true,
            "MissingFieldException must name 'userId' — the decoder could not resolve it against column 'UserId'",
        )

        println("[Test 5] Case sensitivity: column 'UserId' does not satisfy required field 'userId' — MissingFieldException")
        println("[Test 5] Exception: ${ex.message}")
    }

    // ── Test 6: Case sensitivity - decoder, optional field with default ───────

    /**
     * Same casing mismatch as Test 5, but [CaseSensitiveRecordNullable.userId] has an explicit
     * `= null` default. Per [ScdType2ResilienceTest] Test 5, this makes the field genuinely
     * optional: decoding succeeds, and the unresolved field silently takes its default value.
     *
     * The result: `sessionId` (unaffected column, name matches) decodes correctly, but
     * `userId` comes back `null` even though the DataFrame contains a "UserId" column with a
     * real value ("user_1"). No exception, no warning — this is SILENT in exactly the way
     * [ColumnarFormatPositionalDecoderTest] tests 6/7 describe for the positional decoder's
     * same-type field swaps, except here the cause is a column-name casing mismatch rather
     * than column order.
     *
     * The second half of this test shows that [SchemaDriftReport.compare] WOULD have caught
     * this pre-flight (as in Test 4: FIELD_REMOVED("userId") + FIELD_ADDED("UserId")) — i.e.
     * the drift-detection tooling catches what the decoder silently swallows, provided it is
     * actually run before decoding.
     */
    @Test
    @Order(6)
    fun `case sensitivity - decoder silently nulls optional field when column differs only by case`() {
        val df =
            listOf(CaseSensitiveRecordNullable(sessionId = "session_1", userId = "user_1"))
                .toSerializableDataFrame(spark)

        val renamed = df.withColumnRenamed("userId", "UserId")

        val result = renamed.toSerializableKotlinList<CaseSensitiveRecordNullable>()

        assertEquals(1, result.size)
        assertEquals("session_1", result[0].sessionId, "Unaffected column 'sessionId' decodes correctly")
        assertNull(
            result[0].userId,
            "'userId' silently defaults to null — the decoder cannot resolve it against column 'UserId', " +
                "even though that column contains the value \"user_1\"",
        )

        // Pre-flight comparison WOULD have flagged this casing mismatch as drift.
        val expectedSchema = schemaFor(serializer<CaseSensitiveRecordNullable>())
        val actualSchema = renamed.schema()
        val diffs = SchemaDriftReport.compare(expectedSchema, actualSchema)
        val byField = diffs.associateBy { it.fieldName }

        assertEquals(
            DriftKind.FIELD_REMOVED,
            byField["userId"]?.kind,
            "Pre-flight compare() flags 'userId' as missing — this is the check that would catch " +
                "the silent null before decoding, if run",
        )
        assertEquals(
            DriftKind.FIELD_ADDED,
            byField["UserId"]?.kind,
            "Pre-flight compare() flags 'UserId' as an unexpected extra column",
        )

        println("[Test 6] Case sensitivity: 'userId' silently null (column is 'UserId'), but compare() flags both sides")
    }
}
