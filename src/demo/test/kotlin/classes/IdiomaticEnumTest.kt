package classes

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList

class IdiomaticEnumTest : SparkTestBase() {
    data class EnumData(
        val key: String,
        val value: IdiomaticEnum,
    )

    @Test
    fun `test enum with spark dataframe`() {
        val data =
            listOf(
                Pair("A", IdiomaticEnum.SUCCESS),
                Pair("B", IdiomaticEnum.ERROR),
            ).map { (key, value) -> EnumData(key, value) }

        val df = data.toDataFrame(spark)

        assertEquals(2, df.count())
        assertEquals("SUCCESS", df.first().getAs<String>("value"))
    }

    @Test
    fun `test enum with spark dataset`() {
        val data =
            listOf(
                Pair("A", IdiomaticEnum.SUCCESS),
                Pair("B", IdiomaticEnum.ERROR),
            ).map { (key, value) -> EnumData(key, value) }

        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<EnumData>()

        assertEquals(2, results.size)
        assertEquals(IdiomaticEnum.SUCCESS, results.first().value)
    }
}
