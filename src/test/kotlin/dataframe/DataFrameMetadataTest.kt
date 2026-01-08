package dataframe

import classes.SparkTestBase
import integration_pack.toDataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Arrays
import java.util.stream.Collectors

class DataFrameMetadataTest : SparkTestBase() {

    data class Person(var name: String = "", var age: Int = 0)

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data = listOf(Person("Alice", 30), Person("Bob", 40))
        df = data.toDataFrame(spark)
    }

    // --- 6. Metadata and Schema ---

    @Test
    fun `schema() should return the correct StructType`() {
        val schema = df.schema()

        assertEquals(2, schema.fields().size)
        val schemaMap = Arrays.stream(schema.fields())
            .collect(Collectors.toMap({ f -> f.name() }, { f -> f.dataType() }))

        assertEquals(DataTypes.StringType, schemaMap["name"])
        assertEquals(DataTypes.IntegerType, schemaMap["age"])
    }

    @Test
    fun `dtypes() should return column names and their data types`() {
        val dtypes = df.dtypes()

        assertEquals(2, dtypes.size)
        val dtypeMap = Arrays.stream(dtypes)
            .collect(Collectors.toMap({ t -> t._1() }, { t -> t._2() }))

        // Spark Connect returns full type names
        assertEquals("StringType", dtypeMap["name"])
        assertEquals("IntegerType", dtypeMap["age"])
    }

    @Test
    fun `columns() should return all column names`() {
        val columns = df.columns()
        val columnList = columns.toList()
        assertEquals(2, columnList.size)
        assertTrue(columnList.contains("name"))
        assertTrue(columnList.contains("age"))
    }

    @Test
    fun `printSchema() should not throw an exception`() {
        println("--- DataFrame Test: printSchema() ---")
        assertDoesNotThrow { df.printSchema() }
    }

    @Test
    fun `explain() should print the execution plan`() {
        val filteredDF = df.filter("age > 30")

        println("--- DataFrame Test: explain() ---")
        assertDoesNotThrow { filteredDF.explain(true) }
    }

    @Test
    fun `inputFiles() should return an array of file paths`() {
        val files = df.inputFiles()
        assertEquals(0, files.size)
    }
}
