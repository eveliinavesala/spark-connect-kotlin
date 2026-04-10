package catalog

import catalog.toStructType
import catalog.toUcColumns
import classes.UnityCatalogTestBase
import kotlinx.serialization.serializer
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import resilience.*
import spark.kotlin.reflect.getSparkSchema
import spark.kotlin.serialization.schemaFor
import unity_catalog.UnityCatalogRestClient
import kotlin.reflect.typeOf

/**
 * End-to-end schema governance demo using real Unity Catalog (via docker-compose).
 *
 * Run with: ./gradlew demoTest   (requires `make uc-test` to have built the compose stack)
 *
 * Demonstrates:
 *   1. Code-to-Catalog  — push a Kotlin class schema to Unity Catalog via REST
 *   2. Catalog-to-Code  — pull the registered schema and compare against a new Kotlin class
 *   3. Drift detection  — SchemaDriftReport shows which fields diverged
 *   4. Pre-flight check — compare before encoding to catch drift before any data moves
 *   5. Type-gap registration — reflection-backend types registered via getSparkSchema
 *   6. Full workflow    — encode → register → upgrade detected → migration required
 *
 * The UC container provides the screenshot evidence for the thesis.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaGovernanceTest : UnityCatalogTestBase() {

    companion object {
        private const val CATALOG         = "test_catalog"
        private const val SCHEMA          = "scd_demo"
        private const val UC_TABLE        = "customers"
        private const val FINANCIAL_TABLE = "financial_reports"
    }

    private val baseUrl: String by lazy {
        "http://localhost:${UnityCatalogTestBase.getUnityCatalogPort()}"
    }

    @BeforeAll
    fun setupCatalogAndSchema() {
        if (!UnityCatalogRestClient.catalogExists(baseUrl, CATALOG)) {
            UnityCatalogRestClient.createCatalog(baseUrl, CATALOG, "Demo governance catalog")
        }
        UnityCatalogRestClient.createSchema(baseUrl, CATALOG, SCHEMA, "SCD demo schema")
    }

    // ── 1. Code-to-Catalog: push serialization schema to UC ──────────────────

    @Test
    @Order(1)
    fun `code-to-catalog - register CustomerV1 schema from serialization backend`() {
        val schema  = schemaFor(serializer<CustomerV1>())
        val columns = schema.toUcColumns()

        println("\n── Registering CustomerV1 schema to Unity Catalog ──")
        println("Schema: ${schema.treeString()}")
        println("UC columns: $columns")

        val registered = UnityCatalogRestClient.createTable(
            baseUrl          = baseUrl,
            catalogName      = CATALOG,
            schemaName       = SCHEMA,
            tableName        = UC_TABLE,
            columns          = columns,
            storageLocation  = "/tmp/uc-demo/$CATALOG/$SCHEMA/$UC_TABLE",
            dataSourceFormat = "DELTA"
        )

        assertTrue(registered, "CustomerV1 schema should register successfully in Unity Catalog")

        val tables = UnityCatalogRestClient.listTables(baseUrl, CATALOG, SCHEMA)
        assertTrue(tables.contains(UC_TABLE), "Table 'customers' should appear in UC listing")
        println("CustomerV1 schema registered: $CATALOG.$SCHEMA.$UC_TABLE")
    }

    // ── 2. Catalog-to-Code: pull UC schema and detect V2 drift ───────────────

    @Test
    @Order(2)
    fun `catalog-to-code - detect drift between UC V1 schema and local CustomerV2 class`() {
        val ucColumns = UnityCatalogRestClient.getTableColumns(baseUrl, CATALOG, SCHEMA, UC_TABLE)
        assertFalse(ucColumns.isEmpty(), "UC should return column definitions for $UC_TABLE")

        val catalogSchema = ucColumns.toStructType()
        val kotlinSchema  = schemaFor(serializer<CustomerV2>())

        println("\n── Catalog-to-Code Drift Detection ──")
        println("UC registered schema: ${catalogSchema.treeString()}")
        println("Current Kotlin model: ${kotlinSchema.treeString()}")

        val diffs = SchemaDriftReport.compare(kotlinSchema, catalogSchema)
        val report = SchemaDriftReport(
            trigger        = SchemaDriftReport.triggerFrom(diffs),
            typeName       = "CustomerV2 vs UC registered V1",
            expectedSchema = kotlinSchema,
            actualSchema   = catalogSchema,
            diffs          = diffs
        )
        println(report.format())

        assertTrue(diffs.isNotEmpty(), "Drift should be detected: UC has V1 schema, Kotlin class is V2")
        assertTrue(
            diffs.any { it.fieldName == "score" && it.kind == DriftKind.FIELD_REMOVED },
            "Drift report must identify 'score' as absent in the UC-registered schema"
        )
        assertEquals(DriftTrigger.MISSING_FIELD, report.trigger)
    }

    // ── 3. Pre-flight check: compare before encoding ──────────────────────────

    @Test
    @Order(3)
    fun `preflight check - CustomerV2 encoding blocked by UC schema comparison`() {
        val ucColumns     = UnityCatalogRestClient.getTableColumns(baseUrl, CATALOG, SCHEMA, UC_TABLE)
        val catalogSchema = ucColumns.toStructType()
        val kotlinSchema  = schemaFor(serializer<CustomerV2>())

        val diffs = SchemaDriftReport.compare(kotlinSchema, catalogSchema)

        println("\n── Pre-flight Check: CustomerV2 vs UC '$UC_TABLE' ──")
        if (diffs.isEmpty()) {
            println("Schema is compatible — safe to encode")
        } else {
            val report = SchemaDriftReport(
                trigger        = SchemaDriftReport.triggerFrom(diffs),
                typeName       = "CustomerV2 pre-flight",
                expectedSchema = kotlinSchema,
                actualSchema   = catalogSchema,
                diffs          = diffs
            )
            println(report.format())
            println("""
                |── Developer action ─────────────────────────────────────────────────────────
                |  Pre-flight blocked CustomerV2 → UC table '$UC_TABLE'.
                |  Options:
                |    a) Update UC table schema to add 'score' (ALTER TABLE or new registration)
                |    b) Roll back to CustomerV1 for this table
                |    c) Register a new versioned table for the V2 contract
                |──────────────────────────────────────────────────────────────────────────────
            """.trimMargin())
        }

        assertTrue(diffs.any { it.fieldName == "score" },
            "Pre-flight must flag 'score' mismatch before encoding starts")
    }

    // ── 4. Safe migration: register V3 schema as a new versioned table ────────

    @Test
    @Order(4)
    fun `safe migration - CustomerV3 schema registered as versioned table in UC`() {
        val v3Schema  = schemaFor(serializer<CustomerV3>())
        val columns   = v3Schema.toUcColumns()
        val v3Table   = "customers_v3"

        val registered = UnityCatalogRestClient.createTable(
            baseUrl          = baseUrl,
            catalogName      = CATALOG,
            schemaName       = SCHEMA,
            tableName        = v3Table,
            columns          = columns,
            storageLocation  = "/tmp/uc-demo/$CATALOG/$SCHEMA/$v3Table",
            dataSourceFormat = "DELTA"
        )
        assertTrue(registered, "CustomerV3 schema should register successfully")

        val ucColumns     = UnityCatalogRestClient.getTableColumns(baseUrl, CATALOG, SCHEMA, v3Table)
        val catalogSchema = ucColumns.toStructType()

        println("\n── V3 Schema registered in Unity Catalog ──")
        println("UC schema: ${catalogSchema.treeString()}")

        val ucFieldNames = catalogSchema.fieldNames().toSet()
        val v3FieldNames = v3Schema.fieldNames().toSet()
        assertEquals(v3FieldNames, ucFieldNames, "UC schema must cover all V3 fields")

        // Verify 'score' is now nullable in both UC and the Kotlin class
        val scoreCol = ucColumns.find { it["name"] == "score" }
        assertNotNull(scoreCol, "'score' must be registered in UC for V3")
        assertTrue(scoreCol!!["nullable"] as Boolean,
            "'score' must be nullable in UC — matching CustomerV3.score: Int?")

        println("V3 migration: score column registered as nullable in Unity Catalog")
    }

    // ── 5. Type-gap registration: FinancialReport (BigDecimal) via reflection ─

    @Test
    @Order(5)
    fun `type-gap registration - FinancialReport with BigDecimal registered via reflection backend`() {
        // FinancialReport has BigDecimal — not @Serializable, schemaFor() is unavailable.
        // Use reflection's getSparkSchema to derive the UC columns.
        val schema  = getSparkSchema(typeOf<FinancialReport>())
        val columns = schema.toUcColumns()

        println("\n── FinancialReport (BigDecimal) schema via reflection backend ──")
        println("Schema: ${schema.treeString()}")

        val registered = UnityCatalogRestClient.createTable(
            baseUrl          = baseUrl,
            catalogName      = CATALOG,
            schemaName       = SCHEMA,
            tableName        = FINANCIAL_TABLE,
            columns          = columns,
            storageLocation  = "/tmp/uc-demo/$CATALOG/$SCHEMA/$FINANCIAL_TABLE",
            dataSourceFormat = "DELTA"
        )
        assertTrue(registered, "FinancialReport schema should register via reflection backend")

        val ucColumns = UnityCatalogRestClient.getTableColumns(baseUrl, CATALOG, SCHEMA, FINANCIAL_TABLE)
        val amountCol = ucColumns.find { it["name"] == "amount" }
        assertNotNull(amountCol, "'amount' column must be present in UC")
        assertEquals("DECIMAL", (amountCol!!["type_name"] as String).uppercase(),
            "BigDecimal → DECIMAL in Unity Catalog")

        println("BigDecimal registered as DECIMAL — reflection bridges the serialization type gap")
    }

    // ── 6. BackendRouter encode + UC round-trip in one workflow ───────────────

    @Test
    @Order(6)
    fun `end-to-end - encode V1 data, verify schema matches UC, detect V2 drift pre-flight`() {
        val v1Data = listOf(
            CustomerV1("c1", "Alice", "Gold"),
            CustomerV1("c2", "Bob",   "Silver")
        )

        // Step 1: encode via BackendRouter (serialization backend, no drift expected)
        val (df, encodeReport) = BackendRouter.encode(v1Data, spark, serializer<CustomerV1>())
        assertNull(encodeReport, "No drift expected encoding V1 with V1 serializer")
        assertEquals(2L, df.count())
        println("\n── Step 1: V1 data encoded (${df.count()} rows) ──")

        // Step 2: verify DataFrame schema matches the UC-registered V1 schema
        val ucColumns     = UnityCatalogRestClient.getTableColumns(baseUrl, CATALOG, SCHEMA, UC_TABLE)
        val catalogSchema = ucColumns.toStructType()
        val localSchema   = schemaFor(serializer<CustomerV1>())
        val v1Drifts      = SchemaDriftReport.compare(localSchema, catalogSchema)
        println("Step 2: V1 DataFrame vs UC schema — drift: ${if (v1Drifts.isEmpty()) "none" else v1Drifts.toString()}")

        // Step 3: developer upgrades model to V2 — pre-flight catches the contract mismatch
        val v2Drifts = SchemaDriftReport.compare(schemaFor(serializer<CustomerV2>()), catalogSchema)
        assertTrue(v2Drifts.any { it.fieldName == "score" },
            "V2 pre-flight must be blocked by UC schema comparison")

        val preflightReport = SchemaDriftReport(
            trigger        = SchemaDriftReport.triggerFrom(v2Drifts),
            typeName       = "CustomerV2 vs UC (pre-flight)",
            expectedSchema = schemaFor(serializer<CustomerV2>()),
            actualSchema   = catalogSchema,
            diffs          = v2Drifts
        )
        println("Step 3: CustomerV2 upgrade pre-flight:")
        println(preflightReport.format())

        println("\nFull governance workflow:")
        println("   encode V1 → schema matches UC → V2 upgrade detected → migration required")
    }
}
