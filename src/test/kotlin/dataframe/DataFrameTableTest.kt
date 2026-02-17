package dataframe

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import reflection.toDataFrame
import reflection.toKotlinList

class DataFrameTableTest : SparkTestBase() {

    data class Person(var name: String = "", var age: Int = 0)

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data = listOf(
            Person("Alice", 30),
            Person("Bob", 40)
        )
        df = data.toDataFrame(spark)
    }

    @Test
    fun `should save as table and read back`() {
        val tableName = "people_table"
        
        // 1. Save as table
        df.write().mode(SaveMode.Overwrite).saveAsTable(tableName)
        
        // 2. Read back using table()
        val tableDF = spark.table(tableName)
        assertEquals(2L, tableDF.count())
        
        // 3. Read back using SQL
        val sqlDF = spark.sql("SELECT * FROM $tableName")
        assertEquals(2L, sqlDF.count())
        
        // 4. Verify content
        val alice = tableDF.filter("name = 'Alice'").toKotlinList<Person>().first()
        assertEquals(30, alice.age)
        
        // Cleanup
        spark.sql("DROP TABLE IF EXISTS $tableName")
    }

    @Test
    fun `should insert into existing table`() {
        val tableName = "insert_test_table"
        
        // 1. Create initial table
        df.write().mode(SaveMode.Overwrite).saveAsTable(tableName)
        
        // 2. Create new data to insert
        val newPeople = listOf(Person("Charlie", 25)).toDataFrame(spark)
        
        // 3. Insert into table
        newPeople.write().mode(SaveMode.Append).insertInto(tableName)
        
        // 4. Verify total count
        val resultDF = spark.table(tableName)
        assertEquals(3L, resultDF.count())
        
        val charlie = resultDF.filter("name = 'Charlie'").toKotlinList<Person>().first()
        assertEquals(25, charlie.age)
        
        // Cleanup
        spark.sql("DROP TABLE IF EXISTS $tableName")
    }
}
