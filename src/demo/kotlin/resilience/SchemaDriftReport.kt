package resilience

import org.apache.spark.sql.types.StructType

// ── Drift classification ──────────────────────────────────────────────────────

enum class DriftKind {
    /** Field present in Kotlin model but absent in the actual data / DataFrame schema. */
    FIELD_REMOVED,
    /** Field present in actual data but absent in Kotlin model (new column in source). */
    FIELD_ADDED,
    /** Field present in both but types differ. */
    TYPE_CHANGED
}

enum class DriftTrigger {
    /** Non-nullable field in Kotlin model has no corresponding column in the DataFrame. */
    MISSING_FIELD,
    /** Column type in the DataFrame does not match the Kotlin model's expected type. */
    TYPE_MISMATCH,
    /** Field in actual DataFrame not covered by the Kotlin model (SCD column addition). */
    SCD_ADDITION,
    UNKNOWN
}

// ── Report types ──────────────────────────────────────────────────────────────

data class FieldDiff(
    val fieldName: String,
    val expectedType: String?,   // null = field absent from Kotlin model
    val actualType: String?,     // null = field absent from actual DataFrame
    val kind: DriftKind
)

data class SchemaDriftReport(
    val trigger: DriftTrigger,
    val typeName: String,
    val expectedSchema: StructType,
    val actualSchema: StructType,
    val diffs: List<FieldDiff>,
    val cause: Exception? = null
) {
    val hasDrift: Boolean get() = diffs.isNotEmpty()

    fun format(): String = buildString {
        appendLine()
        appendLine("╔══ SCHEMA DRIFT DETECTED [$trigger] — $typeName")
        appendLine("║")
        appendLine("║  Expected schema (Kotlin model):")
        val expectedMap = expectedSchema.fields().associate { it.name() to it.dataType().simpleString() }
        val removedNames = diffs.filter { it.kind == DriftKind.FIELD_REMOVED }.map { it.fieldName }.toSet()
        val changedNames = diffs.filter { it.kind == DriftKind.TYPE_CHANGED }.map { it.fieldName }.toSet()
        expectedSchema.fields().forEach { f ->
            val annotation = when (f.name()) {
                in removedNames -> "  ← REMOVED from data"
                in changedNames -> "  ← TYPE CHANGED"
                else            -> ""
            }
            val nullability = if (f.nullable()) "nullable" else "non-null"
            appendLine("║    ${f.name().padEnd(22)} ${f.dataType().simpleString().padEnd(14)} ($nullability)$annotation")
        }
        appendLine("║")
        appendLine("║  Actual schema (DataFrame / source):")
        val addedNames = diffs.filter { it.kind == DriftKind.FIELD_ADDED }.map { it.fieldName }.toSet()
        actualSchema.fields().forEach { f ->
            val annotation = if (f.name() in addedNames) "  ← NEW (not in Kotlin model)" else ""
            appendLine("║    ${f.name().padEnd(22)} ${f.dataType().simpleString().padEnd(14)}$annotation")
        }
        appendLine("║")
        appendLine("║  Drift summary (${diffs.size} difference${if (diffs.size == 1) "" else "s"}):")
        diffs.forEach { d ->
            val detail = when (d.kind) {
                DriftKind.FIELD_REMOVED -> "expected ${d.expectedType}, absent in data"
                DriftKind.FIELD_ADDED   -> "present in data, absent from model"
                DriftKind.TYPE_CHANGED  -> "model expects ${d.expectedType}, data has ${d.actualType}"
            }
            appendLine("║    ${d.kind.name.padEnd(16)} '${d.fieldName}' — $detail")
        }
        if (cause != null) appendLine("║  Cause: ${cause.javaClass.simpleName}: ${cause.message?.take(120)}")
        append("╚══")
    }

    companion object {
        /**
         * Compare two StructType schemas by field name. Type comparison is case-insensitive
         * to handle differences between backends (e.g. "int" vs "INT").
         */
        fun compare(expected: StructType, actual: StructType): List<FieldDiff> {
            val expectedMap = expected.fields().associate { it.name() to it.dataType().simpleString().lowercase() }
            val actualMap   = actual.fields().associate   { it.name() to it.dataType().simpleString().lowercase() }
            val diffs = mutableListOf<FieldDiff>()

            for ((name, expType) in expectedMap) {
                when (val actType = actualMap[name]) {
                    null    -> diffs += FieldDiff(name, expType, null, DriftKind.FIELD_REMOVED)
                    expType -> { /* match — no drift */ }
                    else    -> diffs += FieldDiff(name, expType, actType, DriftKind.TYPE_CHANGED)
                }
            }
            for ((name, actType) in actualMap) {
                if (name !in expectedMap) {
                    diffs += FieldDiff(name, null, actType, DriftKind.FIELD_ADDED)
                }
            }
            return diffs
        }

        fun triggerFrom(diffs: List<FieldDiff>): DriftTrigger = when {
            diffs.any { it.kind == DriftKind.FIELD_REMOVED } -> DriftTrigger.MISSING_FIELD
            diffs.any { it.kind == DriftKind.TYPE_CHANGED }  -> DriftTrigger.TYPE_MISMATCH
            diffs.any { it.kind == DriftKind.FIELD_ADDED }   -> DriftTrigger.SCD_ADDITION
            else                                              -> DriftTrigger.UNKNOWN
        }
    }
}
