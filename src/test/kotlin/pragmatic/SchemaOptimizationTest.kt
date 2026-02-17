package pragmatic

import classes.SparkTestBase
import reflection.toDataFrame
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SchemaOptimizationTest : SparkTestBase() {

    data class Person(val name: String, val age: Int)

    @Test
    fun `toDataFrame should use provided schema and skip inference`() {
        val data = listOf(Person("Alice", 30))
        
        // Define a custom schema. We change the nullability of 'name' to true (default is false for non-null String).
        // We also add some metadata to verify it's preserved.
        val metadata = Metadata.fromJson("{\"comment\": \"custom schema\"}")
        val customSchema = StructType(arrayOf(
            StructField("name", DataTypes.StringType, true, metadata),
            StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        ))

        val df = data.toDataFrame(spark, schema = customSchema)

        // Verify the schema matches our custom one exactly
        val nameField = df.schema().fields()[0]
        assertEquals(true, nameField.nullable())
        assertEquals("custom schema", nameField.metadata().getString("comment"))
        
        // Verify data is still readable
        assertEquals("Alice", df.first().getAs<String>("name"))
    }
}
