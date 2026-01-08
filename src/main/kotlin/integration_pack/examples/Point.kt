package integration_pack.examples

import org.apache.spark.sql.types.SQLUserDefinedType
import java.io.Serializable

/**
 * A simple class to demonstrate User-Defined Types (UDTs).
 * This class is NOT a data class to show that UDTs can work with arbitrary objects.
 * The annotation links this class to its UDT implementation.
 */
@SQLUserDefinedType(udt = PointUDT::class)
class Point(val x: Double, val y: Double) : Serializable {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Point

        if (x != other.x) return false
        if (y != other.y) return false

        return true
    }

    override fun hashCode(): Int {
        var result = x.hashCode()
        result = 31 * result + y.hashCode()
        return result
    }

    override fun toString(): String {
        return "Point(x=$x, y=$y)"
    }
}
