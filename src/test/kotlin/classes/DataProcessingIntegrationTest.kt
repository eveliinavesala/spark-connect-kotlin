package classes

import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

class DataProcessingIntegrationTest : SparkTestBase() {

    @Test
    fun `test data processing pipeline`() {
        val inputData = listOf(
            IdiomaticDataClass("Charlie", 25),
            IdiomaticDataClass("David", 35)
        )
        val inputDF = spark.createDataFrame(inputData, IdiomaticDataClass::class.java)

        val processedDF = inputDF.withColumn("age", col("age") + 5)

        val outputPath = "build/test-output"
        processedDF.write().mode("overwrite").json(outputPath)

        val outputDF = spark.read().json(outputPath)

        assertEquals(2, outputDF.count())
        assertEquals(30, outputDF.filter(col("name").equalTo("Charlie")).first().getAs<Long>("age"))
    }
}
