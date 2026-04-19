package sparketl

import classes.SparkContainerManager
import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class SparkETLTest : SparkTestBase() {
    companion object {
        private const val CSV_PATH = "/data/spotify_data_clean.csv"
        private lateinit var df: Dataset<Row>

        @BeforeAll
        @JvmStatic
        fun setupData() {
            df =
                SparkContainerManager.sparkSession
                    .read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(CSV_PATH)
                    .cache()
        }
    }

    @Test
    fun `should read csv and infer schema correctly`() {
        assertNotNull(df)
        assertTrue(df.count() > 0)

        val schema = df.schema()
        assertEquals(DataTypes.IntegerType, schema.fields().find { it.name() == "track_popularity" }?.dataType())
        assertEquals(DataTypes.IntegerType, schema.fields().find { it.name() == "artist_followers" }?.dataType())
        assertEquals(DataTypes.StringType, schema.fields().find { it.name() == "explicit" }?.dataType())
    }

    @Test
    fun `should perform basic column selection, renaming, and filtering`() {
        val transformedDF =
            df
                .select("track_name", "artist_name", "track_popularity")
                .withColumnRenamed("artist_name", "artist")
                .filter(col("track_popularity").gt(90))

        val schema = transformedDF.schema()
        val fieldNames = schema.fieldNames().toList()
        assertTrue(fieldNames.containsAll(listOf("track_name", "artist", "track_popularity")))
        assertFalse(fieldNames.contains("artist_name"))

        val count = transformedDF.count()
        assertTrue(count > 0)
        assertTrue(count < df.count())
    }

    @Test
    fun `should create a new column using withColumn`() {
        val transformedDF = df.withColumn("popularity_normalized", col("track_popularity").divide(100.0))

        transformedDF.select("track_popularity", "popularity_normalized").show(5)

        val firstRow = transformedDF.first()
        val popularity = firstRow.getInt(firstRow.fieldIndex("track_popularity"))
        val normalized = firstRow.getDouble(firstRow.fieldIndex("popularity_normalized"))

        assertEquals(popularity / 100.0, normalized, 0.001)
    }

    @Test
    fun `should use a UDF to categorize data`() {
        val durationCategoryUDF =
            UDF1<Int, String> { popularity ->
                when {
                    popularity < 20 -> "Niche"
                    popularity < 60 -> "Mainstream"
                    else -> "Hit"
                }
            }
        spark.udf().register("popularityCategory", durationCategoryUDF, DataTypes.StringType)

        val transformedDF = df.withColumn("popularity_category", callUDF("popularityCategory", col("track_popularity")))

        transformedDF.select("track_name", "track_popularity", "popularity_category").show(5)

        val categories =
            transformedDF
                .select("popularity_category")
                .distinct()
                .collectAsList()
                .map { it.getString(0) }
        assertTrue(categories.containsAll(listOf("Niche", "Mainstream", "Hit")))
    }
}
