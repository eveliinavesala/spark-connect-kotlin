package spark.kotlin.serialization.util

import org.apache.spark.sql.Row
import java.util.AbstractList
import spark.kotlin.serialization.SparkSerializer

/**
 * A [java.util.AbstractList] adapter that defers Row serialization until each element is accessed.
 *
 * Spark's [org.apache.spark.sql.SparkSession.createDataFrame] accepts a Java [java.util.List]<[org.apache.spark.sql.Row]>.
 * By wrapping source data in a lazy list rather than eagerly encoding every element up front,
 * serialization cost is incurred only for rows that Spark actually materialises — relevant when
 * limit or filter push-downs reduce the number of rows consumed.
 */
internal class LazySerializableRowList<T>(
    private val sourceData: List<T>,
    private val serializer: SparkSerializer<T>
) : AbstractList<Row>() {

    override fun get(index: Int): Row = serializer.serialize(sourceData[index])

    override val size: Int get() = sourceData.size
}
