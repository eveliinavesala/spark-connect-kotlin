package integration_pack.examples

import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.*

/**
 * The User-Defined Type (UDT) for the Point class.
 * This class defines how Spark should serialize and deserialize the Point object
 * to and from its internal Row representation.
 */
class PointUDT : UserDefinedType<Point>() {

    override fun sqlType(): DataType {
        // A Point is represented internally as a struct with two doubles.
        return StructType(arrayOf(
            StructField("x", DataTypes.DoubleType, false, Metadata.empty()),
            StructField("y", DataTypes.DoubleType, false, Metadata.empty())
        ))
    }

    override fun serialize(obj: Point): Any {
        // Convert the Point object into a Spark Row.
        return RowFactory.create(obj.x, obj.y)
    }

    override fun deserialize(datum: Any): Point {
        // Convert the Spark Row back into a Point object.
        val row = datum as Row
        return Point(row.getDouble(0), row.getDouble(1))
    }

    override fun userClass(): Class<Point> {
        return Point::class.java
    }
}
