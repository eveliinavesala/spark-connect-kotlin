package spark.kotlin.reflect

import classes.SparkTestBase
import types.Point
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import spark.kotlin.reflect.toKotlinList
import java.nio.file.Path

class DataFrameUDTTest : SparkTestBase() {

    data class City(val name: String, val location: Point)

    @Test
    fun `should correctly handle a User-Defined Type from a file source`(@TempDir tempDir: Path) {
        // 1. Create a DF with the underlying StructType of the UDT
        val rawData = listOf(
            RowFactory.create("Helsinki", RowFactory.create(24.94, 60.17)),
            RowFactory.create("Turku", RowFactory.create(22.26, 60.45))
        )
        val rawSchema = StructType(arrayOf(
            StructField("name", DataTypes.StringType, true, Metadata.empty()),
            StructField("location", StructType(arrayOf(
                StructField("x", DataTypes.DoubleType, false, Metadata.empty()),
                StructField("y", DataTypes.DoubleType, false, Metadata.empty())
            )), true, Metadata.empty())
        ))
        val rawDF = spark.createDataFrame(rawData, rawSchema)

        // 2. Write to Parquet
        val path = tempDir.resolve("cities.parquet").toString()
        rawDF.write().mode(SaveMode.Overwrite).parquet(path)

        // 3. Read back. Schema provision is unnecessary here as Parquet preserves it.
        val udtDF = spark.read().parquet(path)
        udtDF.printSchema()

        // 4. Perform operations on the UDT's internal fields
        val northernCities = udtDF.filter("location.y > 60.2")
        
        // 5. Deserialize back to Kotlin — primary assertion of UDT round-trip correctness.
        val results = northernCities.toKotlinList<City>()
        assertEquals(1, results.size)
        assertEquals("Turku", results.first().name)
        assertEquals(60.45, results.first().location.y)
    }
}
