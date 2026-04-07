package spark.kotlin.reflect

import org.apache.spark.sql.Row
import java.util.AbstractList

internal class LazyRowList(private val sourceData: List<Any>, private val serializer: RowSerializer) : AbstractList<Row>() {
    override fun get(index: Int): Row = serializer.serialize(sourceData[index])
    override val size: Int get() = sourceData.size
}
