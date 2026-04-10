package resilience

import kotlinx.serialization.Serializable
import java.math.BigDecimal

// ── Slowly Changing Dimension domain models ───────────────────────────────────

/**
 * V1 — stable, fully governed contract. Both backends supported.
 */
@Serializable
data class CustomerV1(val id: String, val name: String, val tier: String)

/**
 * V2 — "score" field added as non-nullable (SCD event: field added to model).
 * Decoding V1-shape data (missing "score") fires MissingFieldException in both backends.
 * This proves the Kotlin type system is enforced — silent nulls are rejected.
 */
@Serializable
data class CustomerV2(val id: String, val name: String, val tier: String, val score: Int)

/**
 * V3 — safe SCD migration: "score" made nullable.
 * Decoding V1-shape data (missing "score") returns null; the Kotlin contract now permits it.
 * Both backends accept missing data once the class contract is relaxed.
 */
@Serializable
data class CustomerV3(val id: String, val name: String, val tier: String, val score: Int?)

// ── Type-gap models (reflection backend only) ─────────────────────────────────

/**
 * Contains BigDecimal — not @Serializable because kotlinx has no built-in BigDecimal serializer.
 * Reflection backend handles it via ValueConverters (DecimalType).
 * Use BackendRouter with serializer = null to route directly to reflection.
 */
data class FinancialReport(
    val id: String,
    val amount: BigDecimal,
    val currency: String
)

/**
 * Contains Set<String> — not @Serializable because kotlinx has no SET kind.
 * Reflection backend stores it as an ArrayType column.
 */
data class TaggedItem(
    val id: String,
    val label: String,
    val tags: Set<String>
)

/**
 * Contains java.time.Instant — not @Serializable (no standard kotlinx serializer for java.time).
 * Reflection backend maps it to TimestampType via Timestamp.from().
 */
data class TimeEvent(
    val id: String,
    val description: String,
    val occurredAt: java.time.Instant
)
