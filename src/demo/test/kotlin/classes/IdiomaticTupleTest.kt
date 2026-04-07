package classes

import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IdiomaticTupleTest : SparkTestBase() {

    @Test
    fun `should serialize Kotlin Pair to DataFrame with _1 and _2 columns`() {
        val pairs = listOf(
            "Alice" to 1,
            "Bob" to 2
        )
        
        val df = pairs.toDataFrame(spark)
        df.printSchema()
        df.show()

        val columns = df.columns().toList()
        assertTrue(columns.contains("_1"))
        assertTrue(columns.contains("_2"))
    }

    @Test
    fun `should deserialize Spark Tuple columns (_1, _2) to Kotlin Pair`() {
        val schema = org.apache.spark.sql.types.StructType()
            .add("_1", DataTypes.StringType)
            .add("_2", DataTypes.IntegerType)
            
        val rows = listOf(
            RowFactory.create("Alice", 10),
            RowFactory.create("Bob", 20)
        )
        
        val tupleDF = spark.createDataFrame(rows, schema)
        tupleDF.show()

        val result = tupleDF.toKotlinList<Pair<String, Int>>()
        assertEquals(2, result.size)
        assertEquals("Alice", result[0].first)
        assertEquals(10, result[0].second)
    }

    @Test
    fun `should handle Kotlin Triple with _1, _2, _3 columns`() {
        val triples = listOf(
            Triple("A", 1, true),
            Triple("B", 2, false)
        )
        
        val df = triples.toDataFrame(spark)
        df.printSchema()
        
        val columns = df.columns().toList()
        assertTrue(columns.contains("_1"))
        assertTrue(columns.contains("_2"))
        assertTrue(columns.contains("_3"))

        val result = df.toKotlinList<Triple<String, Int, Boolean>>()
        assertEquals(triples, result)
    }
}
