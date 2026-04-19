package spark.kotlin.reflect

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import spark.kotlin.dsl.times
import spark.kotlin.dsl.transformDF

class DataFrameAdvancedTest : SparkTestBase() {
    data class Person(
        var name: String = "",
        var age: Int = 0,
    )

    data class NameOnly(
        var name: String = "",
    )

    data class AgeDoubled(
        var name: String = "",
        var age: Int = 0,
        var ageDoubled: Long = 0,
    )

    data class FilledPerson(
        var name: String = "",
        var age: Long = 0,
    )

    private lateinit var df: Dataset<Row>

    @BeforeEach
    fun setup() {
        val data = listOf(Person("Alice", 30), Person("Bob", 40))
        df = data.toDataFrame(spark)
    }

    // --- 7. Advanced/Other ---

    @Test
    fun `alias() and as() should set an alias for the dataframe`() {
        val aliased = df.`as`("people_alias")
        val selected = aliased.select("people_alias.name")
        val result = selected.toKotlinList<NameOnly>().first()
        assertEquals("Alice", result.name)
    }

    @Test
    fun `cache(), persist(), and unpersist() should manage caching`() {
        assertDoesNotThrow {
            df.cache()
            assertTrue(df.storageLevel().useMemory())
            df.unpersist()

            df.persist(StorageLevel.MEMORY_ONLY())
            assertTrue(df.storageLevel().useMemory())
            df.unpersist()
        }
    }

    @Test
    fun `checkpoint() and localCheckpoint() should checkpoint the dataframe`() {
        assertDoesNotThrow {
            val localCheckpointed = df.localCheckpoint()
            assertEquals(2L, localCheckpointed.count())
        }
    }

    @Test
    fun `createTempView() and createGlobalTempView() should register views`() {
        df.createOrReplaceTempView("df_view")
        val fromView = spark.sql("SELECT * FROM df_view")
        assertEquals(2L, fromView.count())

        df.createOrReplaceGlobalTempView("df_global_view")
        val fromGlobalView = spark.sql("SELECT * FROM global_temp.df_global_view")
        assertEquals(2L, fromGlobalView.count())
    }

    @Test
    fun `na() should handle missing data`() {
        val dfWithNulls = spark.sql("SELECT 'Alice' as name, 30 as age UNION ALL SELECT null as name, 40 as age")

        val dropped = dfWithNulls.na().drop()
        assertEquals(1L, dropped.count())

        val filled = dfWithNulls.na().fill("Unknown", arrayOf("name"))
        val result = filled.toKotlinList<FilledPerson>().find { it.age == 40L }
        assertEquals("Unknown", result!!.name)
    }

    @Test
    fun `stat() should provide statistical functions`() {
        val approxQuantile = df.stat().approxQuantile("age", doubleArrayOf(0.5), 0.0)
        assertEquals(1, approxQuantile.size)
        assertTrue(approxQuantile[0] >= 30.0)
    }

    @Test
    fun `toJSON() should convert rows to JSON strings`() {
        val jsonDS = df.toJSON()
        val jsonList = jsonDS.collectAsList()
        assertEquals(2, jsonList.size)
        assertTrue(jsonList[0].contains("\"name\":\"Alice\""))
    }

    @Test
    fun `transformDF() should chain custom transformations`() {
        val transformed =
            df.transformDF {
                it.withColumn("ageDoubled", col("age") * 2)
            }
        val result = transformed.toKotlinList<AgeDoubled>().first()
        assertEquals(60, result.ageDoubled)
    }
}
