package pragmatic

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import pragmatic.toDataFrame
import pragmatic.toKotlinList

class PragmaticCollectionsTest : SparkTestBase() {

    // 1. Test for List<Primitive>
    data class ListHolder(val id: Int, val items: List<String>)

    @Test
    fun `should handle List of primitives`() {
        val data = listOf(ListHolder(1, listOf("a", "b", "c")))
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<ListHolder>()
        assertEquals(data, results)
    }

    // 2. Test for Set<Primitive>
    data class SetHolder(val id: Int, val items: Set<String>)

    @Test
    fun `should handle Set of primitives`() {
        val data = listOf(SetHolder(1, setOf("a", "b", "c")))
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<SetHolder>()
        assertEquals(data, results)
    }

    // 3. Test for Map<Primitive, Primitive>
    data class MapHolder(val id: Int, val items: Map<String, Int>)

    @Test
    fun `should handle Map of primitives`() {
        val data = listOf(MapHolder(1, mapOf("a" to 100, "b" to 200)))
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<MapHolder>()
        assertEquals(data, results)
    }
}
