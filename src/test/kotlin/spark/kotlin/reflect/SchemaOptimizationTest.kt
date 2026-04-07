package spark.kotlin.reflect

import classes.SparkTestBase
import spark.kotlin.reflect.toDataFrame
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

        // Custom schema with 'name' nullability overridden to true (inferred default is false for non-null String).
        // Metadata is included to verify it is preserved through the DataFrame round-trip.
        val metadata = Metadata.fromJson("{\"comment\": \"custom schema\"}")
        val customSchema = StructType(arrayOf(
            StructField("name", DataTypes.StringType, true, metadata),
            StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        ))

        val df = data.toDataFrame(spark, schema = customSchema)

        // Verify the schema matches the custom one exactly
        val nameField = df.schema().fields()[0]
        assertEquals(true, nameField.nullable())
        assertEquals("custom schema", nameField.metadata().getString("comment"))

        // Verify data is still readable
        assertEquals("Alice", df.first().getAs<String>("name"))
    }
}
