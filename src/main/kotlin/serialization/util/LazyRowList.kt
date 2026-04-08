package serialization.util

import org.apache.spark.sql.Row
import java.util.AbstractList
import serialization.SparkSerializer

/**
 * Lazy list implementation for efficient DataFrame creation.
 *
 * This list doesn't serialize objects to Rows until they are actually
 * accessed by Spark, avoiding unnecessary work if Spark applies
 * optimizations that don't need all rows.
 */
internal class LazySerializableRowList<T>(
    private val sourceData: List<T>,
    private val serializer: SparkSerializer<T>
) : AbstractList<Row>() {

    override fun get(index: Int): Row = serializer.serialize(sourceData[index])

    override val size: Int get() = sourceData.size
}
