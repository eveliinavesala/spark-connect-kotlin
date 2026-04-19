package catalog

import classes.UnityCatalogTestBase
import kotlinx.serialization.Serializable
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import spark.kotlin.serialization.toSerializableDataFrame
import spark.kotlin.serialization.toSerializableKotlinList

/**
 * Integration tests proving that name-based column resolution in the serialization decoder
 * is correct when Spark DataFrames have a different column order than the Kotlin descriptor.
 *
 * ## Why column order mismatches only surface in production pipelines
 *
 * In a single-session round-trip (encode → decode in the same JVM), the DataFrame is always
 * produced by our own encoder, which preserves declaration order. The decoder never sees a
 * mismatch. The mismatch surfaces only when an external source — Parquet physical layout, Delta
 * schema evolution, Iceberg field-ID ordering, CSV headers, or a catalog table created by
 * another team — controls the column order independently of the Kotlin codebase.
 *
 * ## What is being tested
 *
 * The serialization backend decodes via `schema.fieldIndex(elementName)` (name-based).
 * A position-based decoder would read `row.get(descriptorIndex)`, which produces incorrect
 * results when the DataFrame column at that index does not match the descriptor element at that index.
 *
 * All tests verify that name-based resolution produces correct field values regardless of
 * the column order returned by the external source.
 *
 * ## Scope
 *
 * - ✅ Encoding (Kotlin → Spark via `toSerializableDataFrame`): always safe, not tested here
 * - ✅ Pure Spark operations (filter, groupBy, save): always safe — Test 0 is a control
 * - ❌ Decoding (Spark → Kotlin via `toSerializableKotlinList`): where the mismatch is observable — Tests 1–9
 *
 * Run: `./gradlew demoTest`   (requires `make uc-start` or `make uc-test` for the UC stack)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColumnarFormatColumnOrderingTest : UnityCatalogTestBase() {
    // ── Data models ───────────────────────────────────────────────────────────
    // @Serializable requires class-level declarations — local classes inside
    // functions are not supported by the kotlinx.serialization compiler plugin.

    @Serializable
    data class ColumnOrderTestRecord(
        val userId: String,
        val sessionId: String,
        val timestamp: Long,
        val amount: Double,
    )

    /**
     * V1 schema: three fields, no timestamp.
     * Used in schema-evolution tests to decode rows produced by a V2 schema.
     */
    @Serializable
    data class RecordV1(
        val userId: String,
        val sessionId: String,
        val amount: Double,
    )

    /**
     * Three String fields with distinct semantic roles.
     * All fields share the same Spark type, so a position-based swap produces no type error —
     * only silent semantic corruption.
     */
    @Serializable
    data class StringFieldRecord(
        val userId: String,
        val sessionId: String,
        val requestId: String,
    )

    /** Descriptor order: id (0), name (1), price (2). */
    @Serializable
    data class Product(
        val id: Int,
        val name: String,
        val price: Double,
    )

    /**
     * Descriptor order: username (0), age (1), email (2).
     * Catalog / DDL order: email (0), age (1), username (2).
     */
    @Serializable
    data class User(
        val username: String,
        val age: Int,
        val email: String,
    )

    /** Descriptor order: id (0), amount (1), timestamp (2). */
    @Serializable
    data class Transaction(
        val id: String,
        val amount: Double,
        val timestamp: Long,
    )

    /**
     * Descriptor order: product_id (0), sku (1), quantity (2), warehouse_id (3), last_updated (4).
     * External DDL order: warehouse_id (0), sku (1), quantity (2), last_updated (3), product_id (4).
     * Four of five fields are at different positions.
     */
    @Serializable
    data class InventoryItem(
        val product_id: String,
        val sku: String,
        val quantity: Int,
        val warehouse_id: Int,
        val last_updated: Long,
    )

    // ── Shared test fixtures ──────────────────────────────────────────────────

    private val testRecord =
        ColumnOrderTestRecord(
            userId = "user_12345",
            sessionId = "session_67890",
            timestamp = 1234567890L,
            amount = 99.99,
        )

    // Base path for all file-based tests. Created on the Spark container filesystem.
    private val base = "/tmp/col-order-test"

    // ── Test 0: Control ───────────────────────────────────────────────────────

    /**
     * Establishes that column reordering is invisible to pure Spark operations.
     * The decoder is never invoked, so the subsequent failures are specific to
     * the decode step, not to Spark's column handling in general.
     */
    @Test
    @Order(0)
    fun `control - pure spark pipeline unaffected by column reordering`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test0/original")

        // Reorder to [amount, sessionId, userId, timestamp] via SQL
        val reordered =
            spark.sql(
                """
                SELECT amount, sessionId, userId, timestamp
                FROM parquet.`$base/test0/original`
                """.trimIndent(),
            )

        assertEquals("amount", reordered.schema().fields()[0].name())
        assertEquals("sessionId", reordered.schema().fields()[1].name())

        reordered.write().mode(SaveMode.Overwrite).parquet("$base/test0/reordered")
        val read = spark.read().parquet("$base/test0/reordered")

        // Pure Spark operations — decoder never invoked, column order irrelevant
        assertEquals(1L, read.filter("amount > 50.0").count())
        assertEquals(1L, read.groupBy("userId").count().count())

        println("[Test 0] Pure Spark pipeline: column reordering transparent when decoder not involved")
    }

    // ── Test 1: Parquet explicit reorder ──────────────────────────────────────

    /**
     * Parquet is a columnar format: physical column order in the file can differ from
     * the write order if the file was produced by a different system or tool.
     *
     * `.select()` is used instead of the two-file `mergeSchema` approach because Spark's
     * internal schema merging aligns columns by name before the decoder is invoked,
     * which would mask a position-based decoder's failure. `.select()` forces the mismatch
     * at the decoder interface, exactly where the ordering mismatch is observable.
     *
     * DataFrame schema: [amount, sessionId, userId, timestamp]
     * Descriptor order: [userId, sessionId, timestamp, amount]
     *
     * Position-based failure:
     *   userId    ← pos 0 → 99.99          (reads Double  into String) → type error / cast
     *   sessionId ← pos 1 → "session_67890" (pos 1 lucky match)
     *   timestamp ← pos 2 → "user_12345"   (reads String  into Long)  → type error
     *   amount    ← pos 3 → 1234567890L    (reads Long    into Double) → incorrect value
     */
    @Test
    @Order(1)
    fun `parquet - select reorder exposes decoder mismatch`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test1/data")

        // Confirm declaration order was preserved on write
        val written = spark.read().parquet("$base/test1/data").schema()
        assertEquals("userId", written.fields()[0].name())
        assertEquals("sessionId", written.fields()[1].name())
        assertEquals("timestamp", written.fields()[2].name())
        assertEquals("amount", written.fields()[3].name())

        // .select() reorders at the decoder interface — Spark cannot mask this via mergeSchema
        val reordered =
            spark
                .read()
                .parquet("$base/test1/data")
                .select("amount", "sessionId", "userId", "timestamp")

        // Verify the mismatch is visible at the decoder interface
        assertEquals("amount", reordered.schema().fields()[0].name()) // descriptor expects userId
        assertEquals("sessionId", reordered.schema().fields()[1].name())
        assertEquals("userId", reordered.schema().fields()[2].name()) // descriptor expects timestamp
        assertEquals("timestamp", reordered.schema().fields()[3].name()) // descriptor expects amount

        val result = reordered.toSerializableKotlinList<ColumnOrderTestRecord>()

        assertEquals(1, result.size)
        assertEquals("user_12345", result[0].userId)
        assertEquals("session_67890", result[0].sessionId)
        assertEquals(1234567890L, result[0].timestamp)
        assertEquals(99.99, result[0].amount, 0.001)

        println("[Test 1] Parquet reorder: name-based decoder reads each field by name, not position")
    }

    // ── Test 2: Delta Lake schema evolution ───────────────────────────────────

    /**
     * Delta Lake supports `ALTER TABLE ADD COLUMNS` to evolve the table schema.
     * When a new column is inserted in the middle of the schema, existing readers that
     * hold a reference to the V1 class encounter rows where the column positions of
     * V1 fields have shifted.
     *
     * Production flow:
     *   V1 schema: [userId, sessionId, amount]
     *   ALTER TABLE delta.`path` ADD COLUMNS (timestamp BIGINT AFTER sessionId)
     *   V2 schema: [userId, sessionId, timestamp, amount]  ← timestamp inserted mid-schema
     *
     * Kotlin code retains RecordV1 (no timestamp field):
     *   Position-based: reads timestamp (Long 1234567890) at pos 2 into amount (Double) → corruption
     *   Name-based:     looks up "amount" → finds at pos 3 → 99.99 / 77.77 ✓
     */
    @Test
    @Order(2)
    fun `delta lake schema evolution - mid-schema column insertion`() {
        // Write V1 data
        val v1Records = listOf(RecordV1("user_12345", "session_67890", 99.99))
        v1Records
            .toSerializableDataFrame(spark)
            .write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save("$base/test2/v1")

        val schemaV1 =
            spark
                .read()
                .format("delta")
                .load("$base/test2/v1")
                .schema()
        assertEquals(3, schemaV1.fields().size)
        assertEquals("userId", schemaV1.fields()[0].name())
        assertEquals("sessionId", schemaV1.fields()[1].name())
        assertEquals("amount", schemaV1.fields()[2].name())

        // Write V2 data: timestamp inserted between sessionId and amount.
        // Simulates the result of: ALTER TABLE delta.`v1` ADD COLUMNS (timestamp BIGINT AFTER sessionId)
        val v2Df =
            spark.sql(
                """
                SELECT userId, sessionId,
                       CAST(1234567890 AS BIGINT) AS timestamp,
                       amount
                FROM delta.`$base/test2/v1`
                UNION ALL
                SELECT 'user_99999'    AS userId,
                       'session_11111' AS sessionId,
                       CAST(1234567890 AS BIGINT) AS timestamp,
                       CAST(77.77 AS DOUBLE) AS amount
                """.trimIndent(),
            )

        v2Df
            .write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save("$base/test2/v2")

        // Confirm V2 schema: timestamp at position 2, amount shifted to position 3
        val schemaV2 =
            spark
                .read()
                .format("delta")
                .load("$base/test2/v2")
                .schema()
        assertEquals(4, schemaV2.fields().size)
        assertEquals("userId", schemaV2.fields()[0].name())
        assertEquals("sessionId", schemaV2.fields()[1].name())
        assertEquals("timestamp", schemaV2.fields()[2].name()) // inserted mid-schema
        assertEquals("amount", schemaV2.fields()[3].name()) // shifted from pos 2 → pos 3

        // Kotlin code reads V2 Delta table with V1 class — position-based decoder fails here
        val results =
            spark
                .read()
                .format("delta")
                .load("$base/test2/v2")
                .toSerializableKotlinList<RecordV1>()

        assertEquals(2, results.size)

        val original = checkNotNull(results.find { it.userId == "user_12345" }) { "Row for user_12345 not found" }
        assertEquals(99.99, original.amount, 0.001) // must NOT be 1234567890 (timestamp at pos 2)

        val evolved = checkNotNull(results.find { it.userId == "user_99999" }) { "Row for user_99999 not found" }
        assertEquals(77.77, evolved.amount, 0.001)

        println("[Test 2] Delta schema evolution: V1 decoder reads V2 rows correctly via name lookup")
    }

    // ── Test 3: Iceberg field-ID ordering ─────────────────────────────────────

    /**
     * Iceberg assigns a permanent field ID to each column at table creation time.
     * Physical Parquet files produced by Iceberg order columns by field ID, which can
     * differ from the original declaration order when columns are added, renamed, or
     * reordered through the table's history.
     *
     * This test simulates the field-ID-driven physical layout using SQL column reordering.
     * The technique is identical to the native Iceberg case: a DataFrame whose schema order
     * differs from the descriptor is presented to the decoder.
     *
     * Physical (field-ID) order: [timestamp, amount, userId, sessionId]
     * Descriptor order:          [userId, sessionId, timestamp, amount]
     */
    @Test
    @Order(3)
    fun `iceberg field-id ordering - physical layout independent of descriptor`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test3/original")

        // Simulate physical field-ID ordering via SQL SELECT
        val fieldIdOrdered =
            spark.sql(
                """
                SELECT timestamp, amount, userId, sessionId
                FROM parquet.`$base/test3/original`
                """.trimIndent(),
            )

        assertEquals("timestamp", fieldIdOrdered.schema().fields()[0].name())
        assertEquals("amount", fieldIdOrdered.schema().fields()[1].name())
        assertEquals("userId", fieldIdOrdered.schema().fields()[2].name())
        assertEquals("sessionId", fieldIdOrdered.schema().fields()[3].name())

        fieldIdOrdered.write().mode(SaveMode.Overwrite).parquet("$base/test3/iceberg_sim")

        val result =
            spark
                .read()
                .parquet("$base/test3/iceberg_sim")
                .toSerializableKotlinList<ColumnOrderTestRecord>()

        assertEquals(1, result.size)
        assertEquals(testRecord, result[0])

        println("[Test 3] Iceberg-style: physical field-ID ordering transparent to name-based decoder")
    }

    // ── Test 4: CSV header order ──────────────────────────────────────────────

    /**
     * CSV schema is determined by the header line. When a downstream system or pipeline
     * stage writes a CSV with columns in a different order than the Kotlin descriptor,
     * Spark reads the DataFrame with the CSV's header order. The decoder must map by name.
     *
     * CSV header order: [amount, sessionId, userId, timestamp]
     * Descriptor order: [userId, sessionId, timestamp, amount]
     *
     * Large timestamp (9876543210 > Int.MAX_VALUE) forces LongType inference.
     * Position-based: reads amount (Double at pos 0) into userId (String) → type error.
     */
    @Test
    @Order(4)
    fun `csv header order - name-based resolution follows header, not descriptor`() {
        // Use a timestamp exceeding Int.MAX_VALUE to force LongType inference
        val record = testRecord.copy(timestamp = 9876543210L)

        // Write reordered CSV via Spark — path resolves on the Spark container filesystem
        listOf(record)
            .toSerializableDataFrame(spark)
            .select("amount", "sessionId", "userId", "timestamp")
            .coalesce(1)
            .write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv("$base/test4/reordered_csv")

        val df =
            spark
                .read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("$base/test4/reordered_csv")

        // Schema reflects CSV header order, not descriptor order
        assertEquals("amount", df.schema().fields()[0].name())
        assertEquals("sessionId", df.schema().fields()[1].name())
        assertEquals("userId", df.schema().fields()[2].name())
        assertEquals("timestamp", df.schema().fields()[3].name())

        // Spark's CSV writer may quote numeric values — assert inferred types are numeric,
        // not StringType. If this fails in practice, switch to an explicit schema on the read.
        assertEquals(
            "double",
            df
                .schema()
                .fields()[0]
                .dataType()
                .simpleString(),
        ) // amount
        assertEquals(
            "bigint",
            df
                .schema()
                .fields()[3]
                .dataType()
                .simpleString(),
        ) // timestamp (> Int.MAX_VALUE → LongType)

        val result = df.toSerializableKotlinList<ColumnOrderTestRecord>()

        assertEquals(1, result.size)
        assertEquals("user_12345", result[0].userId)
        assertEquals("session_67890", result[0].sessionId)
        assertEquals(9876543210L, result[0].timestamp)
        assertEquals(99.99, result[0].amount, 0.001)

        println("[Test 4] CSV: header-driven schema decoded correctly, long value preserved")
    }

    // ── Test 5: Unity Catalog schema enforcement ──────────────────────────────

    /**
     * Unity Catalog enforces schema at the table level, independent of Kotlin descriptor
     * definitions. Schema evolution through SQL DDL by another team can insert columns
     * mid-schema without any awareness in the Kotlin codebase.
     *
     * Production flow:
     *   ALTER TABLE test_catalog.col_order.products ADD COLUMN category STRING AFTER name
     *   INSERT INTO test_catalog.col_order.products VALUES (2, 'Gadget', 'Electronics', 29.99)
     *
     * Evolved catalog schema: [id, name, category, price]   ← category inserted at pos 2
     * Original Product class: [id, name, price]
     *
     * Position-based: reads category (String at pos 2) into price (Double) → type error.
     * Name-based:     looks up "price" by name → finds at pos 3 → 19.99 / 29.99. ✓
     */
    @Test
    @Order(5)
    fun `unity catalog - external team inserts compliance column mid-schema`() {
        // Represents what spark.table("test_catalog.col_order.products") returns after
        // ALTER TABLE ADD COLUMN category STRING AFTER name
        val evolvedCatalogDf =
            spark.sql(
                """
                SELECT 1 AS id, 'Widget' AS name, CAST(NULL AS STRING) AS category, 19.99 AS price
                UNION ALL
                SELECT 2 AS id, 'Gadget' AS name, 'Electronics'        AS category, 29.99 AS price
                """.trimIndent(),
            )

        val schema = evolvedCatalogDf.schema()
        assertEquals("id", schema.fields()[0].name())
        assertEquals("name", schema.fields()[1].name())
        assertEquals("category", schema.fields()[2].name()) // inserted by external team
        assertEquals("price", schema.fields()[3].name()) // shifted from pos 2 → pos 3

        // Team A's original Product class — no category field, reads with name-based decoder
        val result = evolvedCatalogDf.toSerializableKotlinList<Product>()

        assertEquals(2, result.size)
        assertEquals(19.99, result.find { it.id == 1 }!!.price, 0.001)
        assertEquals(29.99, result.find { it.id == 2 }!!.price, 0.001)

        println("[Test 5] UC schema evolution: Product.price resolved at pos 3 by name, not pos 2 (category)")
    }

    // ── Test 6: Catalog-enforced column order ─────────────────────────────────

    /**
     * A Unity Catalog table's column order is determined by the DDL that created it.
     * When the Kotlin team did not create the table themselves — it was created by another
     * team, a migration script, or dbt — the catalog column order can differ from the
     * Kotlin descriptor.
     *
     * UC DDL order:    email (0), age (1), username (2)
     * Descriptor order: username (0), age (1), email (2)
     *
     * Position-based:
     *   username ← pos 0 → "alice@example.com"  (WRONG — email value, same type, NO ERROR)
     *   age      ← pos 1 → 30                   (correct by coincidence)
     *   email    ← pos 2 → "alice"               (WRONG — username value, same type, NO ERROR)
     * Silent corruption: no exception, semantically wrong data.
     *
     * Name-based: all fields correct. ✓
     */
    @Test
    @Order(6)
    fun `unity catalog - catalog column order overrides descriptor order`() {
        // Represents what spark.table("test_catalog.col_order.users") returns after:
        //   CREATE TABLE test_catalog.col_order.users (email STRING, age INT, username STRING) USING DELTA
        //   INSERT INTO test_catalog.col_order.users VALUES ('alice@example.com', 30, 'alice')
        val catalogDf =
            spark.sql(
                """
                SELECT 'alice@example.com' AS email, 30 AS age, 'alice' AS username
                """.trimIndent(),
            )

        assertEquals("email", catalogDf.schema().fields()[0].name()) // catalog pos 0
        assertEquals("age", catalogDf.schema().fields()[1].name()) // catalog pos 1
        assertEquals("username", catalogDf.schema().fields()[2].name()) // catalog pos 2

        // User descriptor has username at pos 0, email at pos 2 — opposite of catalog
        val users = catalogDf.toSerializableKotlinList<User>()

        assertEquals(1, users.size)
        assertEquals("alice", users[0].username) // descriptor pos 0, catalog pos 2
        assertEquals(30, users[0].age)
        assertEquals("alice@example.com", users[0].email) // descriptor pos 2, catalog pos 0

        println("[Test 6] Catalog order: User decoded correctly, no username/email swap")
    }

    // ── Test 7: Silent string corruption ─────────────────────────────────────

    /**
     * When position-based decoding swaps fields of the same type, there is no type error,
     * no exception, and no visible failure — only semantically incorrect data.
     * This is the most dangerous failure mode because it is undetectable without domain-level
     * validation of the output values.
     *
     * Written order:   [userId, sessionId, requestId]
     * Physical layout: [requestId, userId, sessionId]
     *
     * Position-based (all three fields are String — no type check catches this):
     *   userId    ← pos 0 → "request_11111"  (WRONG, valid String, no error)
     *   sessionId ← pos 1 → "user_12345"     (WRONG, valid String, no error)
     *   requestId ← pos 2 → "session_67890"  (WRONG, valid String, no error)
     */
    @Test
    @Order(7)
    fun `silent string corruption - same-type swap produces no exception`() {
        val original =
            StringFieldRecord(
                userId = "user_12345",
                sessionId = "session_67890",
                requestId = "request_11111",
            )

        // Write with declaration order [userId, sessionId, requestId]
        listOf(original)
            .toSerializableDataFrame(spark)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("$base/test7/original")

        // Reorder to [requestId, userId, sessionId] — simulates external system column ordering
        spark
            .sql(
                """
                SELECT requestId, userId, sessionId
                FROM parquet.`$base/test7/original`
                """.trimIndent(),
            ).write()
            .mode(SaveMode.Overwrite)
            .parquet("$base/test7/reordered")

        val result =
            spark
                .read()
                .parquet("$base/test7/reordered")
                .toSerializableKotlinList<StringFieldRecord>()

        assertEquals(1, result.size)
        // Name-based: all three read correctly — no values swapped
        assertEquals("user_12345", result[0].userId)
        assertEquals("session_67890", result[0].sessionId)
        assertEquals("request_11111", result[0].requestId)

        println("[Test 7] Silent corruption: name-based decoder prevents same-type field swap")
    }

    // ── Test 8: Multi-team catalog workflow ───────────────────────────────────

    /**
     * In a shared data platform, multiple teams work on the same catalog table.
     * Team A (Kotlin) writes transactions with schema [id, amount, timestamp].
     * Team B (Data Engineering / Compliance) adds audit columns via SQL DDL — without
     * touching Team A's Kotlin code.
     *
     * After Team B's ALTER TABLE:
     *   [id, user_id, region, amount, timestamp]  ← two audit columns inserted after id
     *
     * Team A reads with the original Transaction class, unaware of the new columns.
     * Position-based: reads user_id (String at pos 1) into amount (Double) → type error.
     * Name-based: finds "amount" at pos 3, "timestamp" at pos 4 → correct values. ✓
     */
    @Test
    @Order(8)
    fun `multi-team catalog - compliance columns shift transaction field positions`() {
        // Represents spark.table("prod.finance.transactions") after Team B's ALTER TABLE:
        //   ADD COLUMNS (user_id STRING AFTER id, region STRING AFTER user_id)
        val auditSchemaDf =
            spark.sql(
                """
                SELECT 'txn_001' AS id,
                       CAST(NULL    AS STRING) AS user_id,
                       CAST(NULL    AS STRING) AS region,
                       99.99 AS amount,
                       CAST(1234567890 AS BIGINT) AS timestamp
                UNION ALL
                SELECT 'txn_002' AS id,
                       'user_123'  AS user_id,
                       'EU'        AS region,
                       49.99 AS amount,
                       CAST(1234567900 AS BIGINT) AS timestamp
                """.trimIndent(),
            )

        val schema = auditSchemaDf.schema()
        assertEquals("id", schema.fields()[0].name())
        assertEquals("user_id", schema.fields()[1].name()) // Team B compliance column
        assertEquals("region", schema.fields()[2].name()) // Team B compliance column
        assertEquals("amount", schema.fields()[3].name()) // shifted from pos 1 → pos 3
        assertEquals("timestamp", schema.fields()[4].name()) // shifted from pos 2 → pos 4

        val transactions = auditSchemaDf.toSerializableKotlinList<Transaction>()

        assertEquals(2, transactions.size)
        assertEquals(99.99, transactions.find { it.id == "txn_001" }!!.amount, 0.001)
        assertEquals(49.99, transactions.find { it.id == "txn_002" }!!.amount, 0.001)

        println("[Test 8] Multi-team: Team A's decoder unaffected by Team B's audit column insertions")
    }

    // ── Test 9: Infrastructure-as-code table creation ─────────────────────────

    /**
     * Infrastructure-as-code tools (Terraform, dbt, Pulumi) create catalog tables with a
     * column order driven by their own conventions — alphabetical, dependency order, or DDL
     * template order — independent of the Kotlin data class declaration order.
     *
     * IaC DDL order:    warehouse_id (0), sku (1), quantity (2), last_updated (3), product_id (4)
     * Descriptor order: product_id (0), sku (1), quantity (2), warehouse_id (3), last_updated (4)
     *
     * Four of five fields are at different positions.
     * Position-based decoder fails for product_id (reads warehouse_id Int into String field)
     * and last_updated (reads product_id String into Long field) → type errors.
     * Name-based decoder maps all five fields correctly. ✓
     */
    @Test
    @Order(9)
    fun `infrastructure-as-code table - arbitrary column order vs descriptor order`() {
        // Represents spark.table("external.warehouse.inventory") as created by an IaC tool:
        //   CREATE TABLE external.warehouse.inventory (
        //       warehouse_id INT, sku STRING, quantity INT,
        //       last_updated BIGINT, product_id STRING
        //   ) USING DELTA
        //   INSERT INTO ... VALUES (1, 'SKU-001', 100, 1234567890, 'PROD-001')
        val externalToolDf =
            spark.sql(
                """
                SELECT 1              AS warehouse_id,
                       'SKU-001'      AS sku,
                       100            AS quantity,
                       CAST(1234567890 AS BIGINT) AS last_updated,
                       'PROD-001'     AS product_id
                """.trimIndent(),
            )

        val schema = externalToolDf.schema()
        assertEquals("warehouse_id", schema.fields()[0].name()) // descriptor expects this at pos 3
        assertEquals("sku", schema.fields()[1].name())
        assertEquals("quantity", schema.fields()[2].name())
        assertEquals("last_updated", schema.fields()[3].name()) // descriptor expects this at pos 4
        assertEquals("product_id", schema.fields()[4].name()) // descriptor expects this at pos 0

        val items = externalToolDf.toSerializableKotlinList<InventoryItem>()

        assertEquals(1, items.size)
        assertEquals("PROD-001", items[0].product_id)
        assertEquals("SKU-001", items[0].sku)
        assertEquals(100, items[0].quantity)
        assertEquals(1, items[0].warehouse_id)
        assertEquals(1234567890L, items[0].last_updated)

        println("[Test 9] IaC schema: all 5 fields resolved correctly by name despite position mismatch")
    }
}
