package pragmatic

import classes.SparkTestBase
import integration_pack.UDFs
import integration_pack.toDataFrame
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class PragmaticUDFTest : SparkTestBase() {

    data class Song(val artist: String, val title: String)

    private val songs = listOf(
        Song("The Beatles", "Hey Jude"),
        Song("Led Zeppelin", "Stairway to Heaven")
    )

    @BeforeEach
    fun setupUdf() {
        // Register the UDF from our global object
        spark.udf().register("upperUDF", UDFs.upper)
    }

    @Test
    fun `pragmatic approach can register and use a named UDF`() {
        val df = songs.toDataFrame(spark)
        val result = df.withColumn("upper_artist", callUDF("upperUDF", col("artist"))).collectAsList()
        assertEquals("THE BEATLES", result[0].getAs("upper_artist"))
    }

    @Test
    fun `pragmatic approach can use an anonymous UDF with withColumn`() {
        val df = songs.toDataFrame(spark)
        // Use the UDF object's apply method explicitly
        val result = df.withColumn("upper_title", UDFs.upper.apply(col("title"))).collectAsList()
        assertEquals("HEY JUDE", result[0].getAs("upper_title"))
    }
}
