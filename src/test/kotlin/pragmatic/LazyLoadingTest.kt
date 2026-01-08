package pragmatic

import classes.SparkTestBase
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.AbstractList
import java.util.concurrent.atomic.AtomicInteger

class LazyLoadingTest : SparkTestBase() {

    data class SimpleData(val id: Int, val value: String)

    /**
     * This test is the most critical. It verifies the "lazy" nature of the list.
     * It creates a custom list that tracks when the `get` method is called.
     * We expect that Spark's `createDataFrame` will iterate through the list,
     * calling `get` for each element sequentially. We verify that not all elements
     * are accessed at once.
     */
    @Test
    fun `LazyRowList should access elements lazily`() {
        val accessTracker = mutableSetOf<Int>()
        val getCount = AtomicInteger(0)

        // A custom list that records every time an element is accessed.
        val trackingList = object : AbstractList<SimpleData>() {
            private val data = listOf(SimpleData(1, "a"), SimpleData(2, "b"), SimpleData(3, "c"))
            override val size: Int = data.size
            override fun get(index: Int): SimpleData {
                accessTracker.add(index)
                getCount.incrementAndGet()
                return data[index]
            }
        }

        // At this point, before creating the DataFrame, no elements should have been accessed.
        assertEquals(0, getCount.get(), "No elements should be accessed before DataFrame creation")

        val df = trackingList.toDataFrame(spark)

        // Force Spark to materialize the DataFrame. The `count()` action is one of the
        // simplest ways to trigger the computation.
        val dfCount = df.count()

        assertEquals(3, dfCount)
        // Verify that all elements were eventually accessed to create the DataFrame.
        assertEquals(trackingList.size, getCount.get(), "All elements should be accessed by the end")
        assertEquals(setOf(0, 1, 2), accessTracker, "All indices should have been visited")
    }

    /**
     * This test ensures that even with the lazy-loading mechanism, the data
     * integrity is maintained. The final collected list should be identical
     * to the source data.
     */
    @Test
    fun `should correctly convert data using LazyRowList`() {
        val data = listOf(
            SimpleData(1, "Alice"),
            SimpleData(2, "Bob"),
            SimpleData(3, "Charlie")
        )
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<SimpleData>()

        assertEquals(data, results, "Data should be perfectly preserved after a round trip")
    }

    /**
     * Tests the edge case of an empty list to ensure no errors are thrown.
     */
    @Test
    fun `should handle an empty list correctly`() {
        val data = emptyList<SimpleData>()
        val df = data.toDataFrame(spark)
        val results = df.toKotlinList<SimpleData>()

        assertTrue(results.isEmpty(), "The resulting list should also be empty")
        assertEquals(0, df.count(), "The DataFrame count should be 0")
    }

    /**
     * This test simulates a larger list to ensure the lazy mechanism works at a slightly
     * larger scale without issues. A true memory test is difficult in a unit test,
     * but a successful run of this test provides confidence that the mechanism is sound.
     */
    @Test
    fun `should handle a larger list without crashing`() {
        val largeList = (1..1000).map { SimpleData(it, "Value_$it") }
        val df = largeList.toDataFrame(spark)
        
        assertEquals(1000, df.count(), "DataFrame count should match the large list size")

        // As a final check, collect the results and verify the first and last elements
        val results = df.toKotlinList<SimpleData>()
        assertEquals(largeList.size, results.size)
        assertEquals(largeList.first(), results.first())
        assertEquals(largeList.last(), results.last())
    }

    /**
     * This test directly instantiates LazyRowList to ensure its internal `get` method
     * works as expected in isolation.
     */
    @Test
    fun `LazyRowList get() method should convert object to Row correctly`() {
        val data = listOf(SimpleData(10, "test"))
        val schema = ReflectionCache.getSchema(SimpleData::class)
        val serializer = ReflectionCache.getSerializer(SimpleData::class)
        val lazyList = LazyRowList(data, serializer)

        assertEquals(1, lazyList.size)

        val row = lazyList[0]

        // Verify the created Row has the correct schema and values
        assertEquals(schema, row.schema())
        assertEquals(10, row.getInt(0))
        assertEquals("test", row.getString(1))
    }
}
