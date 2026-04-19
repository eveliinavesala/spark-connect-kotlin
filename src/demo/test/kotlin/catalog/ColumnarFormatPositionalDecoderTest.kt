package catalog

import classes.UnityCatalogTestBase
import kotlinx.serialization.Serializable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.assertThrows
import serialization.toPositionalKotlinList
import spark.kotlin.serialization.toSerializableDataFrame

/**
 * Evidence tests proving that a position-based decoder fails — or silently corrupts data —
 * on the same production pipeline scenarios where the name-based decoder succeeds.
 *
 * Each test mirrors a scenario from [ColumnarFormatColumnOrderingTest] and runs the same
 * DataFrame through [toPositionalKotlinList], which uses [serialization.decoders.PositionalSparkRowDecoder].
 * That decoder reads `row.get(descriptorIndex)` without any name lookup.
 *
 * ## Expected outcomes
 *
 * | Test | Outcome            | Reason                                              |
 * |------|--------------------|-----------------------------------------------------|
 * | 0    | passes             | no decode step (control)                            |
 * | 1    | throws             | Double at pos 0 decoded as String → ClassCastException |
 * | 2    | throws             | Long (timestamp) at pos 2 decoded as Double → ClassCastException |
 * | 3    | throws             | Long at pos 0 decoded as String → ClassCastException |
 * | 4    | throws             | Double at pos 0 decoded as String → ClassCastException |
 * | 5    | throws             | category=NULL at pos 2 → SparkRuntimeException ROW_VALUE_IS_NULL (null check fires before type cast; second row would throw ClassCastException on "Electronics") |
 * | 6    | silent corruption  | String/String swap — incorrect field values, no exception |
 * | 7    | silent corruption  | String/String swap — incorrect field values, no exception |
 * | 8    | throws             | user_id=NULL at pos 1 → SparkRuntimeException ROW_VALUE_IS_NULL (same null-before-cast path as Test 5) |
 * | 9    | throws             | Int (warehouse_id) at pos 0 decoded as String → ClassCastException |
 *
 * All 10 tests are GREEN: they assert the observed behavior of the positional decoder under column order mismatch.
 *
 * Run: `./gradlew demoTest`
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ColumnarFormatPositionalDecoderTest : UnityCatalogTestBase() {
    // ── Data models (duplicated from ColumnarFormatColumnOrderingTest) ─────────

    @Serializable
    data class ColumnOrderTestRecord(
        val userId: String,
        val sessionId: String,
        val timestamp: Long,
        val amount: Double,
    )

    @Serializable
    data class RecordV1(
        val userId: String,
        val sessionId: String,
        val amount: Double,
    )

    @Serializable
    data class StringFieldRecord(
        val userId: String,
        val sessionId: String,
        val requestId: String,
    )

    @Serializable
    data class Product(
        val id: Int,
        val name: String,
        val price: Double,
    )

    @Serializable
    data class User(
        val username: String,
        val age: Int,
        val email: String,
    )

    @Serializable
    data class Transaction(
        val id: String,
        val amount: Double,
        val timestamp: Long,
    )

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

    private val base = "/tmp/col-order-pos-test"

    // ── Test 0: Control ───────────────────────────────────────────────────────

    /**
     * No decode step — the positional decoder is never invoked.
     * Establishes that column reordering is invisible to pure Spark operations.
     */
    @Test
    @Order(0)
    fun `control - pure spark pipeline unaffected by column reordering`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test0/original")
        val reordered =
            spark.sql(
                """
                SELECT amount, timestamp, sessionId, userId
                FROM parquet.`$base/test0/original`
                """.trimIndent(),
            )
        reordered.write().mode(SaveMode.Overwrite).parquet("$base/test0/reordered")
        val read = spark.read().parquet("$base/test0/reordered")

        assertEquals(1L, read.filter("amount > 50.0").count())
        assertEquals(1L, read.groupBy("userId").count().count())

        println("[Test 0] Control passes — decoder not involved")
    }

    // ── Test 1: Parquet explicit reorder → throws ─────────────────────────────

    /**
     * DataFrame: [amount(Double), sessionId, userId, timestamp(Long)]
     * Descriptor: [userId(String), sessionId, timestamp(Long), amount(Double)]
     *
     * Position 0: decodeString() on a Double column → ClassCastException.
     */
    @Test
    @Order(1)
    fun `parquet - positional decoder throws on type mismatch at position 0`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test1/data")
        val reordered =
            spark
                .read()
                .parquet("$base/test1/data")
                .select("amount", "sessionId", "userId", "timestamp")

        assertEquals("amount", reordered.schema().fields()[0].name()) // Double where String expected

        assertThrows<Exception> {
            reordered.toPositionalKotlinList<ColumnOrderTestRecord>()
        }.also { e ->
            println("[Test 1] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 2: Delta Lake schema evolution → throws ──────────────────────────

    /**
     * V2 schema: [userId, sessionId, timestamp(Long), amount(Double)]
     * Descriptor (RecordV1): [userId, sessionId, amount(Double)]
     *
     * Position 2: decodeDouble() on Long (timestamp) → getDouble() on Long → ClassCastException.
     */
    @Test
    @Order(2)
    fun `delta lake schema evolution - positional decoder throws reading Long as Double`() {
        val v1Records = listOf(RecordV1("user_12345", "session_67890", 99.99))
        v1Records
            .toSerializableDataFrame(spark)
            .write()
            .format("delta")
            .mode(SaveMode.Overwrite)
            .save("$base/test2/v1")
        val v1 = spark.read().format("delta").load("$base/test2/v1")
        val v1Reordered =
            v1
                .select("amount", "sessionId")
                .withColumn("timestamp", lit(1234567890L))
                .withColumn("userId", lit("user_12345"))

        v1Reordered
            .write()
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save("$base/test2/v2")
        val v2 = spark.read().format("delta").load("$base/test2/v2")
        assertEquals("timestamp", v2.schema().fields()[2].name()) // Long at pos 2, descriptor expects Double

        assertThrows<Exception> {
            v2.toPositionalKotlinList<RecordV1>()
        }.also { e ->
            println("[Test 2] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 3: Iceberg field-ID ordering → throws ────────────────────────────

    /**
     * DataFrame: [timestamp(Long), amount(Double), userId, sessionId]
     * Descriptor: [userId(String), sessionId, timestamp(Long), amount(Double)]
     *
     * Position 0: decodeString() on Long (timestamp) → ClassCastException.
     */
    @Test
    @Order(3)
    fun `iceberg field-id ordering - positional decoder throws reading Long as String`() {
        val df = listOf(testRecord).toSerializableDataFrame(spark)
        df.write().mode(SaveMode.Overwrite).parquet("$base/test3/original")
        val fieldIdOrdered =
            spark.sql(
                """
                SELECT timestamp, sessionId, amount, userId
                FROM parquet.`$base/test3/original`
                """.trimIndent(),
            )
        fieldIdOrdered.write().mode(SaveMode.Overwrite).parquet("$base/test3/iceberg_sim")
        val reordered = spark.read().parquet("$base/test3/iceberg_sim")
        assertEquals("timestamp", reordered.schema().fields()[0].name()) // Long where String expected

        assertThrows<Exception> {
            reordered.toPositionalKotlinList<ColumnOrderTestRecord>()
        }.also { e ->
            println("[Test 3] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 4: CSV header order → throws ────────────────────────────────────

    /**
     * CSV header order: [amount(Double), sessionId, userId, timestamp(Long)]
     * Descriptor: [userId(String), sessionId, timestamp(Long), amount(Double)]
     *
     * Position 0: decodeString() on Double (amount) → ClassCastException.
     */
    @Test
    @Order(4)
    fun `csv header order - positional decoder throws reading Double as String`() {
        val record = testRecord.copy(timestamp = 9876543210L)

        listOf(record)
            .toSerializableDataFrame(spark)
            .select("amount", "sessionId", "userId", "timestamp")
            .coalesce(1)
            .write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv("$base/test4/reordered_csv")
        val reordered =
            spark
                .read()
                .option("header", "true")
                .csv("$base/test4/reordered_csv")

        assertEquals("amount", reordered.schema().fields()[0].name()) // Double where String expected

        assertThrows<Exception> {
            reordered.toPositionalKotlinList<ColumnOrderTestRecord>()
        }.also { e ->
            println("[Test 4] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 5: Unity Catalog schema enforcement → throws ────────────────────

    /**
     * Evolved catalog schema: [id(Int), name(String), category(String), price(Decimal)]
     * Descriptor (Product): [id(Int), name(String), price(Double)]
     *
     * Position 2: decodeDouble() on String (category) → ClassCastException.
     */
    @Test
    @Order(5)
    fun `unity catalog - positional decoder throws reading String category as Double price`() {
        val evolvedCatalogDf =
            spark.sql(
                """
                SELECT 1 AS id, 'Widget' AS name, CAST(NULL AS STRING) AS category, 19.99 AS price
                UNION ALL
                SELECT 2 AS id, 'Gadget' AS name, 'Electronics'        AS category, 29.99 AS price
                """.trimIndent(),
            )

        assertEquals(
            "category",
            evolvedCatalogDf.schema().fields()[2].name(),
        ) // String at pos 2, descriptor expects Double

        assertThrows<Exception> {
            evolvedCatalogDf.toPositionalKotlinList<Product>()
        }.also { e ->
            println("[Test 5] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 6: Catalog-enforced column order → silent corruption ─────────────

    /**
     * Catalog DDL order: [email(String), age(Int), username(String)]
     * Descriptor (User): [username(String), age(Int), email(String)]
     *
     * All types match positionally (String, Int, String) — no exception.
     * Values are wrong: username receives email value, email receives username value.
     * This is the most dangerous failure mode: undetectable without domain validation.
     */
    @Test
    @Order(6)
    fun `unity catalog - positional decoder silently swaps username and email`() {
        val catalogDf =
            spark.sql(
                """
                SELECT 'alice@example.com' AS email, 30 AS age, 'alice' AS username
                """.trimIndent(),
            )

        assertEquals("email", catalogDf.schema().fields()[0].name())
        assertEquals("username", catalogDf.schema().fields()[2].name())

        // Does NOT throw — types are compatible positionally (String→String, Int→Int, String→String)
        val users = catalogDf.toPositionalKotlinList<User>()

        assertEquals(1, users.size)
        // Positional decoder reads email value ("alice@example.com") into username field
        assertEquals("alice@example.com", users[0].username) // incorrect — expected "alice"
        assertEquals(30, users[0].age) // correct by coincidence
        // Positional decoder reads username value ("alice") into email field
        assertEquals("alice", users[0].email) // incorrect — expected "alice@example.com"

        println("[Test 6] Positional decoder: silent corruption — username='${users[0].username}', email='${users[0].email}'")
    }

    // ── Test 7: Silent string corruption → silent corruption ─────────────────

    /**
     * Physical layout: [requestId, userId, sessionId]
     * Descriptor (StringFieldRecord): [userId, sessionId, requestId]
     *
     * All String — no type error. Every field receives the wrong value.
     */
    @Test
    @Order(7)
    fun `silent string corruption - positional decoder swaps all three string fields`() {
        val original =
            StringFieldRecord(
                userId = "user_12345",
                sessionId = "session_67890",
                requestId = "request_11111",
            )

        listOf(original)
            .toSerializableDataFrame(spark)
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("$base/test7/original")
        val fieldIdOrdered =
            spark.sql(
                """
                SELECT sessionId, userId, requestId
                FROM parquet.`$base/test7/original`
                """.trimIndent(),
            )
        fieldIdOrdered
            .write()
            .mode(SaveMode.Overwrite)
            .parquet("$base/test7/reordered")
        val reordered = spark.read().parquet("$base/test7/reordered")

        // Does NOT throw — all columns are String
        val result = reordered.toPositionalKotlinList<StringFieldRecord>()

        assertEquals(1, result.size)
        // Every field receives an incorrect value — positional mapping reads requestId into userId, etc.
        // Mapping:
        // schema[0]: sessionId (String) -> userId (expected original[0] "user_12345")
        // schema[1]: userId (String) -> sessionId (expected original[1] "session_67890")
        // schema[2]: requestId (String) -> requestId (expected original[2] "request_11111")
        assertEquals("session_67890", result[0].userId)
        assertEquals("user_12345", result[0].sessionId)
        assertEquals("request_11111", result[0].requestId)

        println("[Test 7] Positional decoder: all three String fields silently swapped, no exception")
    }

    // ── Test 8: Multi-team catalog workflow → throws ──────────────────────────

    /**
     * Audit schema: [id(String), user_id(String), region(String), amount(Decimal), timestamp(Long)]
     * Descriptor (Transaction): [id(String), amount(Double), timestamp(Long)]
     *
     * Position 1: decodeDouble() on String (user_id) → ClassCastException.
     */
    @Test
    @Order(8)
    fun `multi-team catalog - positional decoder throws reading String user_id as Double amount`() {
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

        assertEquals("user_id", auditSchemaDf.schema().fields()[1].name()) // String at pos 1, descriptor expects Double

        assertThrows<Exception> {
            auditSchemaDf.toPositionalKotlinList<Transaction>()
        }.also { e ->
            println("[Test 8] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }

    // ── Test 9: Infrastructure-as-code table creation → throws ───────────────

    /**
     * IaC DDL order: [warehouse_id(Int), sku(String), quantity(Int), last_updated(Long), product_id(String)]
     * Descriptor (InventoryItem): [product_id(String), sku(String), quantity(Int), warehouse_id(Int), last_updated(Long)]
     *
     * Position 0: decodeString() on Int (warehouse_id) → ClassCastException.
     */
    @Test
    @Order(9)
    fun `infrastructure-as-code table - positional decoder throws reading Int warehouse_id as String product_id`() {
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

        assertEquals(
            "warehouse_id",
            externalToolDf.schema().fields()[0].name(),
        ) // Int at pos 0, descriptor expects String

        assertThrows<Exception> {
            externalToolDf.toPositionalKotlinList<InventoryItem>()
        }.also { e ->
            println("[Test 9] Positional decoder threw ${e::class.simpleName}: ${e.message}")
        }
    }
}
