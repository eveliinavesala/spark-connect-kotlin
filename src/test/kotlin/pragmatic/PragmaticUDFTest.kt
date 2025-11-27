package pragmatic

import classes.SparkTestBase
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PragmaticUDFTest : SparkTestBase() {

    @Test
    fun `pragmatic approach can register and use a named UDF`() {
        val data = listOf("a", "b", "c")
        val df = spark.createDataset(data, org.apache.spark.sql.Encoders.STRING()).toDF("value")

        val toUpperCaseUDF = UDF1<String, String> { str -> str.uppercase() }
        spark.udf().register("myUpperCase", toUpperCaseUDF, DataTypes.StringType)

        val transformedDF = df.selectExpr("myUpperCase(value) as upper_value")
        val results = transformedDF.collectAsList().map { it.getString(0) }

        assertEquals(listOf("A", "B", "C"), results)
    }

    @Test
    fun `pragmatic approach can use an anonymous UDF with withColumn`() {
        val data = listOf("a", "b", "c")
        val df = spark.createDataset(data, org.apache.spark.sql.Encoders.STRING()).toDF("value")

        // 1. Define a Kotlin lambda.
        val toLowerCaseLambda = UDF1<String, String> { str -> str.lowercase() }

        // 2. Wrap the lambda with the udf function factory.
        val toLowerCaseUDF = udf(toLowerCaseLambda, DataTypes.StringType)

        // 3. Use the UDF's .apply() method to call it on a column.
        val transformedDF = df.withColumn("lower_value", toLowerCaseUDF.apply(col("value")))

        // 4. Assert the results.
        val results = transformedDF.select("lower_value").collectAsList().map { it.getString(0) }
        assertEquals(listOf("a", "b", "c"), results)
    }
}
