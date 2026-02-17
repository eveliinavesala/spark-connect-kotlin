package reflection

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import kotlin.reflect.KProperty1

// ============================================================================
// A) Column Operators & Infix DSL
// ============================================================================

operator fun Column.times(other: Any): Column = this.multiply(other)
operator fun Column.div(other: Any): Column = this.divide(other)
operator fun Column.get(field: String): Column = this.getField(field)

infix fun Column.eq(other: Any?): Column = this.equalTo(other)
infix fun Column.neq(other: Any?): Column = this.notEqual(other)
infix fun Column.gt(other: Any?): Column = this.gt(other)
infix fun Column.gte(other: Any?): Column = this.geq(other)
infix fun Column.lt(other: Any?): Column = this.lt(other)
infix fun Column.lte(other: Any?): Column = this.leq(other)
infix fun Column.and(other: Column): Column = this.and(other)
infix fun Column.or(other: Column): Column = this.or(other)

// ============================================================================
// B) Dataset Extensions
// ============================================================================

/**
 * Pretty-print a DataFrame or Dataset.
 */
fun <T> Dataset<T>.showPretty(n: Int = 20, truncate: Boolean = false) = this.show(n, truncate)

/**
 * Chainable show() method. Useful for debugging intermediate steps.
 */
fun <T> Dataset<T>.showDS(n: Int = 20, truncate: Boolean = false): Dataset<T> {
    this.show(n, truncate)
    return this
}

/**
 * Scoped caching. Automatically unpersists the dataset after the block executes.
 * Uses blocking unpersist to ensure resources are freed immediately.
 */
inline fun <T, R> Dataset<T>.withCached(block: (Dataset<T>) -> R): R {
    this.cache()
    return try {
        block(this)
    } finally {
        this.unpersist(true) // Blocking unpersist for deterministic behavior
    }
}

/**
 * DataFrame-specific transform extension for chaining transformations.
 */
fun Dataset<Row>.transformDF(t: (Dataset<Row>) -> Dataset<Row>): Dataset<Row> {
    // Explicitly specify both type arguments: <Element Type, Return Dataset Type>
    return this.transform<Row, Dataset<Row>> { v1 -> t(v1) }
}

/**
 * Selects columns in a type-safe way using Kotlin property references.
 */
inline fun <reified T : Any> Dataset<Row>.selectTyped(
    vararg properties: KProperty1<T, *>
): Dataset<Row> {
    val columns = properties.map { col(it.name) }.toTypedArray()
    return this.select(*columns)
}

// ============================================================================
// C) Row Accessors
// ============================================================================

inline fun <reified T> Row.getNullable(name: String): T? =
    if (this.isNullAt(this.fieldIndex(name))) null else this.getAs<T>(name)

// ============================================================================
// D) Lifecycle Wrappers
// ============================================================================

/**
 * Helper to manage the SparkSession lifecycle.
 */
inline fun <T> withSpark(
    master: String = "sc://localhost:15002",
    block: (SparkSession) -> T
): T {
    val spark = SparkSession.builder()
        .remote(master)
        .getOrCreate()
    return try {
        block(spark)
    } finally {
        spark.stop()
    }
}
