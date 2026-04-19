package unitycatalog

import classes.UnityCatalogTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class UnityCatalogIntegrationTest : UnityCatalogTestBase() {
    data class SalesRecord(
        val id: Int,
        val product: String,
        val revenue: Double,
        val region: String,
    )

    companion object {
        private var catalogSetupDone = false

        /**
         * Ensure Unity Catalog test environment is set up.
         * This must be called after containers are started (i.e., after accessing spark).
         */
        fun ensureCatalogSetup() {
            if (catalogSetupDone) return

            println("--- Setting up Unity Catalog test catalog and schema ---")

            // Get the Unity Catalog port from the running container
            val ucPort = getUnityCatalogPort()
            val baseUrl = "http://localhost:$ucPort"

            // Create test catalog and schema via REST API
            val catalogCreated =
                UnityCatalogRestClient.createCatalog(
                    baseUrl = baseUrl,
                    name = "test_catalog",
                    comment = "Test catalog for integration tests",
                )

            val schemaCreated =
                UnityCatalogRestClient.createSchema(
                    baseUrl = baseUrl,
                    catalogName = "test_catalog",
                    schemaName = "sales_data",
                    comment = "Sales data schema for integration tests",
                )

            if (!catalogCreated || !schemaCreated) {
                println("WARNING: Failed to create catalog or schema via REST API")
            } else {
                println("Unity Catalog test environment ready (catalog: test_catalog, schema: sales_data)")
            }

            catalogSetupDone = true
        }
    }

    @Test
    @Order(1)
    fun `should verify Unity Catalog is accessible via REST API`() {
        ensureCatalogSetup()

        val ucPort = getUnityCatalogPort()
        val baseUrl = "http://localhost:$ucPort"

        // List catalogs via REST API
        val catalogs = UnityCatalogRestClient.listCatalogs(baseUrl)

        println("Available catalogs via REST API: $catalogs")

        // Verify test_catalog exists
        assertTrue(
            catalogs.contains("test_catalog"),
            "test_catalog should be available via REST API. Available: $catalogs",
        )
    }

    @Test
    @Order(2)
    fun `should verify test schema exists in Unity Catalog via REST API`() {
        ensureCatalogSetup()

        val ucPort = getUnityCatalogPort()
        val baseUrl = "http://localhost:$ucPort"

        // List schemas in test_catalog via REST API
        val schemas = UnityCatalogRestClient.listSchemas(baseUrl, "test_catalog")

        println("Schemas in test_catalog via REST API: $schemas")

        assertTrue(
            schemas.contains("sales_data"),
            "sales_data schema should exist in test_catalog",
        )
    }

    @Test
    @Order(3)
    fun `should create table in Unity Catalog via REST API`() {
        ensureCatalogSetup()

        val ucPort = getUnityCatalogPort()
        val baseUrl = "http://localhost:$ucPort"

        // Create table via REST API
        val tableCreated =
            UnityCatalogRestClient.createTable(
                baseUrl = baseUrl,
                catalogName = "test_catalog",
                schemaName = "sales_data",
                tableName = "sales_records",
                columns =
                    listOf(
                        mapOf("name" to "id", "type_name" to "INT", "nullable" to false, "position" to 0),
                        mapOf("name" to "product", "type_name" to "STRING", "nullable" to false, "position" to 1),
                        mapOf("name" to "revenue", "type_name" to "DOUBLE", "nullable" to false, "position" to 2),
                        mapOf("name" to "region", "type_name" to "STRING", "nullable" to false, "position" to 3),
                    ),
                storageLocation = "/tmp/unity-catalog/test_catalog/sales_data/sales_records",
                dataSourceFormat = "DELTA",
            )

        assertTrue(tableCreated, "Table should be created successfully via REST API")

        // Verify table exists
        val tables = UnityCatalogRestClient.listTables(baseUrl, "test_catalog", "sales_data")
        println("Tables in sales_data: $tables")

        assertTrue(
            tables.contains("sales_records"),
            "sales_records table should exist in Unity Catalog",
        )
    }

    @Test
    @Order(4)
    fun `should list tables in Unity Catalog via REST API`() {
        ensureCatalogSetup()

        val ucPort = getUnityCatalogPort()
        val baseUrl = "http://localhost:$ucPort"

        // Ensure table exists from previous test
        val tables = UnityCatalogRestClient.listTables(baseUrl, "test_catalog", "sales_data")

        println("Tables in test_catalog.sales_data: $tables")

        assertTrue(tables.isNotEmpty(), "Should have at least one table")
    }

    @Test
    @Order(5)
    fun `should get table metadata from Unity Catalog via REST API`() {
        ensureCatalogSetup()

        val ucPort = getUnityCatalogPort()
        val baseUrl = "http://localhost:$ucPort"

        // Get table info
        val tableInfo =
            UnityCatalogRestClient.getTable(
                baseUrl,
                "test_catalog",
                "sales_data",
                "sales_records",
            )

        println("Table info: $tableInfo")

        assertNotNull(tableInfo, "Should retrieve table information")
        assertEquals("sales_records", tableInfo?.get("name"))
        assertEquals("test_catalog", tableInfo?.get("catalog_name"))
        assertEquals("sales_data", tableInfo?.get("schema_name"))
    }

    @Test
    @Order(6)
    fun `generateCreateSQL should work with Unity Catalog naming`() {
        // This is a unit test - doesn't require UC to be running
        val sql =
            UnityCatalogIntegrator.generateCreateSQL<SalesRecord>(
                tableName = "test_catalog.sales_data.products",
                format = "DELTA",
            )

        println("Generated SQL:\n$sql")

        assertTrue(sql.contains("CREATE TABLE test_catalog.sales_data.products"))
        assertTrue(sql.contains("id INT"))
        assertTrue(sql.contains("product STRING"))
        assertTrue(sql.contains("revenue DOUBLE"))
        assertTrue(sql.contains("region STRING"))
        assertTrue(sql.contains("USING DELTA"))
    }
}
