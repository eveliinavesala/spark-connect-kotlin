package pragmatic

import classes.IdiomaticDataClass
import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PragmaticTest : SparkTestBase() {

    @Test
    fun `test data class with pragmatic dataframe approach`() {
        val data = listOf(
            IdiomaticDataClass("Alice", 30),
            IdiomaticDataClass("Bob", 40)
        )

        // 1. Use the new extension function to create a DataFrame (Dataset<Row>)
        val df = spark.createPragmaticDataFrame(data, IdiomaticDataClass::class)

        // Perform transformations using the DataFrame API
        val filteredDF = df.filter("age > 35")

        // 2. Use the new extension function to convert results back to a Kotlin List
        val results = filteredDF.toKotlinList(IdiomaticDataClass::class)

        // 3. Assert the results
        assertEquals(1, results.size)
        assertEquals("Bob", results[0].name)
    }
}
