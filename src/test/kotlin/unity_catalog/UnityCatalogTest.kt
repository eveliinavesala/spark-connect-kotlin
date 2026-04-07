package unity_catalog

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class UnityCatalogTest : SparkTestBase() {

    data class Product(val id: Int, val name: String, val price: Double)

    @Test
    fun `generateCreateSQL should produce valid DDL`() {
        val sql = UnityCatalogIntegrator.generateCreateSQL<Product>("main.default.products")
        
        println(sql)
        
        assertTrue(sql.contains("CREATE TABLE main.default.products"))
        assertTrue(sql.contains("id INT")) // Spark SQL type for IntegerType is INT or INTEGER
        assertTrue(sql.contains("name STRING"))
        assertTrue(sql.contains("price DOUBLE"))
        assertTrue(sql.contains("USING DELTA"))
    }

    @Test
    fun `registerTable should execute without error`() {
        // A simple table name is used rather than a qualified name (e.g. "main.default"),
        // as the latter may not exist in the test container environment.
        val tableName = "products_table"
        
        // Use PARQUET because the test container might not have Delta Lake installed
        UnityCatalogIntegrator.registerTable<Product>(spark, tableName, format = "PARQUET")
        
        val tables = spark.sql("SHOW TABLES").collectAsList()
        assertTrue(tables.any { it.getString(1) == tableName }) // tableName is usually in 2nd column (tableName)
        
        // Verify schema
        val df = spark.table(tableName)
        val schema = df.schema()
        assertTrue(schema.fieldNames().contains("id"))
        assertTrue(schema.fieldNames().contains("name"))
    }
}
