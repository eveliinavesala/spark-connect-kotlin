package pragmatic

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes

/**
 * A collection of globally accessible, serializable UDFs.
 * Defining them as lazy ensures they are created only when a Spark session is active.
 */
object UDFs {
    val upper: UserDefinedFunction by lazy {
        udf(
            UDF1 { s: String -> s.uppercase() }, 
            DataTypes.StringType
        )
    }

    val categorizePopularity: UserDefinedFunction by lazy {
        udf(
            { popularity: Int ->
                when {
                    popularity > 60 -> "Global Hit"
                    popularity > 30 -> "Mainstream"
                    else -> "Niche"
                }
            }, DataTypes.StringType
        )
    }
}
