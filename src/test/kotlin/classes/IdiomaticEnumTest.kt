package classes

import org.apache.spark.sql.Encoders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class IdiomaticEnumTest : SparkTestBase() {

    data class EnumData(val key: String, val value: IdiomaticEnum)

    @Test
    fun `test enum with spark dataframe`() {
        val data = listOf(
            Pair("A", IdiomaticEnum.SUCCESS),
            Pair("B", IdiomaticEnum.ERROR)
        ).map { (key, value) -> EnumData(key, value) }

        val df = spark.createDataFrame(data, EnumData::class.java)

        assertEquals(2, df.count())
        assertEquals("SUCCESS", df.first().getAs<String>("value"))
    }

    @Test
    fun `test enum with spark dataset`() {
        val data = listOf(
            Pair("A", IdiomaticEnum.SUCCESS),
            Pair("B", IdiomaticEnum.ERROR)
        ).map { (key, value) -> EnumData(key, value) }

        val ds = spark.createDataset(data, Encoders.bean(EnumData::class.java))

        assertEquals(2, ds.count())
        assertEquals(IdiomaticEnum.SUCCESS, ds.first().value)
    }
}
