package resilience

import classes.SparkTestBase
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.TimestampType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import java.math.BigDecimal
import java.time.Instant

/**
 * Demonstrates the type coverage gap between the serialization and reflection backends.
 *
 * Types that cannot be @Serializable (BigDecimal, Set<T>, java.time.Instant, etc.)
 * are routed directly to the reflection backend via BackendRouter with serializer = null.
 *
 * This is not a fallback from failure — it is an acknowledged design decision:
 * the reflection backend is the authoritative path for types without a KSerializer.
 * The BackendRouter makes this routing explicit and auditable.
 *
 * Types demonstrated:
 *   - BigDecimal   → DecimalType (reflection: java.math.BigDecimal → Spark DecimalType)
 *   - Set<String>  → ArrayType   (reflection: Set stored as array; order not preserved)
 *   - java.time.Instant → TimestampType (reflection: via java.sql.Timestamp)
 */
class TypeCoverageGapTest : SparkTestBase() {
    // ── BigDecimal ────────────────────────────────────────────────────────────

    @Test
    fun `BigDecimal field routes to reflection backend - schema has DecimalType`() {
        val reports =
            listOf(
                FinancialReport("r1", BigDecimal("1234.56"), "EUR"),
                FinancialReport("r2", BigDecimal("9999.99"), "USD"),
            )

        // Type gap acknowledged — serializer = null routes to reflection
        val (df, report) = BackendRouter.encode(reports, spark, serializer = null)

        assertNull(report, "No drift report when routing to reflection intentionally")

        val amountField = df.schema().fields().find { it.name() == "amount" }
        assertNotNull(amountField, "DataFrame must contain 'amount' column")
        assertTrue(
            amountField!!.dataType() is DecimalType,
            "BigDecimal must map to DecimalType — reflection backend converts via java.math.BigDecimal",
        )

        println("BigDecimal → ${amountField.dataType()} (reflection backend only)")
        println(df.schema().treeString())
    }

    @Test
    fun `BigDecimal round-trip via reflection backend preserves value`() {
        val original =
            listOf(
                FinancialReport("r1", BigDecimal("1234.56789"), "EUR"),
            )

        val df = original.toDataFrame(spark)
        val decoded = df.toKotlinList<FinancialReport>()

        assertEquals(1, decoded.size)
        assertEquals(original[0].id, decoded[0].id)
        assertEquals(original[0].currency, decoded[0].currency)
        // BigDecimal scale may differ after Spark round-trip; compare by value
        assertEquals(
            0,
            original[0].amount.compareTo(decoded[0].amount),
            "BigDecimal value must be preserved after Spark round-trip",
        )
    }

    // ── Set<String> ───────────────────────────────────────────────────────────

    @Test
    fun `Set field routes to reflection backend - schema has ArrayType`() {
        val items =
            listOf(
                TaggedItem("i1", "Kotlin", setOf("jvm", "functional", "oop")),
                TaggedItem("i2", "Spark", setOf("distributed", "batch")),
            )

        val (df, report) = BackendRouter.encode(items, spark, serializer = null)

        assertNull(report)

        val tagsField = df.schema().fields().find { it.name() == "tags" }
        assertNotNull(tagsField)
        assertTrue(
            tagsField!!.dataType() is ArrayType,
            "Set<String> must map to ArrayType — kotlinx has no SET kind, reflection uses ArrayType",
        )

        println("Set<String> → ${tagsField.dataType()} (reflection backend only)")
    }

    @Test
    fun `Set round-trip via reflection backend preserves elements`() {
        val original =
            listOf(
                TaggedItem("i1", "Test", setOf("alpha", "beta", "gamma")),
            )

        val df = original.toDataFrame(spark)
        val decoded = df.toKotlinList<TaggedItem>()

        assertEquals(1, decoded.size)
        assertEquals(
            original[0].tags,
            decoded[0].tags,
            "Set elements must be preserved after round-trip (order may differ, Set equality handles this)",
        )
    }

    // ── java.time.Instant ────────────────────────────────────────────────────

    @Test
    fun `java_time_Instant field routes to reflection backend - schema has TimestampType`() {
        val events =
            listOf(
                TimeEvent("e1", "System started", Instant.parse("2025-01-01T10:00:00Z")),
                TimeEvent("e2", "Job completed", Instant.parse("2025-01-01T11:30:00Z")),
            )

        val (df, report) = BackendRouter.encode(events, spark, serializer = null)

        assertNull(report)

        val timestampField = df.schema().fields().find { it.name() == "occurredAt" }
        assertNotNull(timestampField)
        assertTrue(
            timestampField!!.dataType() is TimestampType,
            "java.time.Instant must map to TimestampType via java.sql.Timestamp (reflection only)",
        )

        println("java.time.Instant → ${timestampField.dataType()} (reflection backend only)")
        println(df.schema().treeString())
    }

    @Test
    fun `java_time_Instant round-trip via reflection backend preserves timestamp`() {
        val originalInstant = Instant.parse("2025-06-15T08:45:30Z")
        val original = listOf(TimeEvent("e1", "Event", originalInstant))

        val df = original.toDataFrame(spark)
        val decoded = df.toKotlinList<TimeEvent>()

        assertEquals(1, decoded.size)
        // Spark stores timestamps at microsecond precision; truncate to second for comparison
        assertEquals(
            originalInstant.epochSecond,
            decoded[0].occurredAt.epochSecond,
            "Instant epoch seconds must be preserved after round-trip",
        )
    }

    // ── Capability matrix summary ─────────────────────────────────────────────

    @Test
    fun `type coverage gap - summary of types requiring reflection backend`() {
        println(
            """
            |
            |── Type Coverage Gap Summary ────────────────────────────────────────────────────
            |  Type                 Reflection      Serialization   Notes
            |  ─────────────────── ─────────────── ─────────────── ────────────────────────────
            |  BigDecimal           yes: DecimalType no              No standard KSerializer
            |  Set<T>               yes: ArrayType   no              No SET kind in kotlinx
            |  java.time.Instant    yes: Timestamp   no              No kotlinx-datetime bridge
            |  java.time.LocalDate  yes: DateType    no              Same
            |  Array<T>             yes: ArrayType   no              No Array SerialKind
            |  ByteArray            yes: BinaryType  no              No standard KSerializer
            |  Pair<A, B>           yes: StructType  no              No standard KSerializer
            |  Generic Box<T>       yes              no              KType reified; descriptor is not
            |
            |  BackendRouter with serializer = null routes all of the above to reflection.
            |  The router call site documents the gap explicitly in source code.
            |──────────────────────────────────────────────────────────────────────────────────
            """.trimMargin(),
        )
        // No assertion — this test exists to print the summary and act as living documentation
        assertTrue(true)
    }
}
