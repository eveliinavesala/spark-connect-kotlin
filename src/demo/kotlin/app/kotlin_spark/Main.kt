package app.kotlin_spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

fun main() {
    // Reverting to connect to a local Spark Connect server
    val spark = SparkSession.builder()
        .remote("sc://localhost:15002")
        .getOrCreate()

    println("Successfully connected to local Spark Connect server.")

    // Define schema
    val schema = StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType)
        .add("age", DataTypes.IntegerType)
        .add("city", DataTypes.StringType)

    // Create dirty data as List<Row>
    val dirtyRows = listOf(
        RowFactory.create(1, "John Doe", 25, "Helsinki"),
        RowFactory.create(2, "  Jane Smith  ", 30, "Turku"),
        RowFactory.create(1, "John Doe", 25, "Helsinki"), // duplicate
        RowFactory.create(3, "Bob", -5, "Tampere"), // invalid age
        RowFactory.create(4, null, 35, "Oulu"), // missing name
        RowFactory.create(5, "Eve", 150, "Espoo"), // invalid age
        RowFactory.create(6, "Charlie", 22, null) // missing city
    )

    val df = spark.createDataFrame(dirtyRows, schema)

    println("=== ORIGINAL DATA ===")
    df.show()
    println("Count: ${df.count()}")

    // Remove duplicates
    val noDupes = df.dropDuplicates()
    println("\n=== AFTER DEDUPLICATION ===")
    println("Count: ${noDupes.count()}")

    // Filter valid ages and non-null names
    val cleaned = noDupes
        .filter("age > 0 AND age < 120")
        .filter("name IS NOT NULL")
        .na().drop("any", arrayOf("city")) // drop rows with null city

    println("\n=== CLEANED DATA ===")
    cleaned.show()
    println("Count: ${cleaned.count()}")

    spark.stop()
}
