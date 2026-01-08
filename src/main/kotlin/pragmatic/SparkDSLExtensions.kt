package pragmatic

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import scala.Function1

// A) Kotlin Extension Functions for Columns
operator fun Column.times(other: Any): Column = this.multiply(other)
operator fun Column.div(other: Any): Column = this.divide(other)
operator fun Column.get(field: String): Column = this.getField(field)

// B) Kotlin Extensions for Dataset

/**
 * Convert a DataFrame (Dataset<Row>) to a typed Dataset.
 * Use this sparingly if you need typed operations, but prefer DataFrame API when possible.
 */
inline fun <reified T : Any> Dataset<Row>.to(): Dataset<T> =
    this.`as`(Encoders.bean(T::class.java))

/**
 * Pretty-print a DataFrame or Dataset.
 */
fun <T> Dataset<T>.showPretty(n: Int = 20, truncate: Boolean = false) = this.show(n, truncate)

/**
 * DataFrame-specific transform extension for chaining transformations.
 * Renamed to transformDF to avoid conflict with the native Java transform method.
 * 
 * Example:
 * ```
 * df.transformDF { it.withColumn("doubled", col("value") * 2) }
 *   .transformDF { it.filter(col("doubled") > 10) }
 * ```
 */
fun Dataset<Row>.transformDF(t: (Dataset<Row>) -> Dataset<Row>): Dataset<Row> {
    val scalaFunc = object : Function1<Dataset<Row>, Dataset<Row>> {
        override fun apply(v1: Dataset<Row>): Dataset<Row> {
            return t(v1)
        }
    }
    // We explicitly call the Java transform method here, helping the compiler with types
    return this.transform<Row, Dataset<Row>>(scalaFunc)
}

// C) Typed Row Accessors
// Provides a nullable, type-safe accessor to avoid shadowing the existing `getAs`
inline fun <reified T> Row.getNullable(name: String): T? =
    if (this.isNullAt(this.fieldIndex(name))) null else this.getAs<T>(name)
