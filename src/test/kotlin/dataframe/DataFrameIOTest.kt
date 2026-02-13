package dataframe

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class DataFrameIOTest : SparkTestBase() {

    data class Person(var name: String = "", var age: Int = 0)

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data = listOf(Person("Alice", 30), Person("Bob", 40))
        df = data.toDataFrame(spark)
    }

    @Test
    fun `should write and read Parquet files`(@TempDir tempDir: Path) {
        val path = tempDir.resolve("people.parquet").toString()
        
        // Write
        df.write().mode(SaveMode.Overwrite).parquet(path)
        
        // Read
        val readDF = spark.read().parquet(path)
        
        assertEquals(2L, readDF.count())
        assertEquals(2, readDF.columns().size)
        // Parquet preserves schema, so types should match. Name is string, Age is int.
        val aliceRow = readDF.filter("name = 'Alice'").first()
        assertEquals("Alice", aliceRow.getAs<String>("name"))
        assertEquals(30, aliceRow.getAs<Int>("age"))
    }

    @Test
    fun `should write and read CSV files with options`(@TempDir tempDir: Path) {
        val path = tempDir.resolve("people.csv").toString()
        
        // Write with header
        df.write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(path)
        
        // Read with header
        val readDF = spark.read()
            .option("header", "true")
            .option("inferSchema", "true") // Infer types to get Int for age
            .csv(path)
        
        assertEquals(2L, readDF.count())
        assertEquals(2, readDF.columns().size)
        val bobRow = readDF.filter("name = 'Bob'").first()
        assertEquals("Bob", bobRow.getAs<String>("name"))
        assertEquals(40, bobRow.getAs<Int>("age"))
    }

    @Test
    fun `should write and read JSON files`(@TempDir tempDir: Path) {
        val path = tempDir.resolve("people.json").toString()
        
        // Write
        df.write().mode(SaveMode.Overwrite).json(path)
        
        // Read
        val readDF = spark.read().json(path)
        
        assertEquals(2L, readDF.count())
        val aliceRow = readDF.filter("name = 'Alice'").first()
        assertEquals("Alice", aliceRow.getAs<String>("name"))
        // JSON might infer Long for numbers, so be careful with types
        assertEquals(30, aliceRow.getAs<Number>("age").toInt())
    }
}
