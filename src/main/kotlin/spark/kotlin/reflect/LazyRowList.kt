package spark.kotlin.reflect

import org.apache.spark.sql.Row
import java.util.AbstractList

/**
 * A [java.util.AbstractList] adapter that defers Row serialization until each element is accessed.
 *
 * Spark's [org.apache.spark.sql.SparkSession.createDataFrame] accepts a Java [java.util.List]<[Row]>.
 * By wrapping the source data in a lazy list rather than eagerly converting every element up front,
 * serialization cost is incurred only for rows that Spark actually materialises — relevant when
 * limit or filter push-downs reduce the number of rows read.
 */
internal class LazyRowList(
    private val sourceData: List<Any>,
    private val serializer: RowSerializer,
) : AbstractList<Row>() {
    override fun get(index: Int): Row = serializer.serialize(sourceData[index])

    override val size: Int get() = sourceData.size
}
