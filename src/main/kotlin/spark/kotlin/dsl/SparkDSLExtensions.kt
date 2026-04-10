package spark.kotlin.dsl

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import kotlin.reflect.KProperty1

// ============================================================================
// A) Column Operators & Infix DSL
// ============================================================================

/**
 * Kotlin `*` operator mapped to [Column.multiply].
 *
 * Spark's `Column.$times` is a Scala operator not exposed to Kotlin; this operator function
 * bridges the gap so `col("price") * col("qty")` compiles and behaves as Spark multiplication.
 */
operator fun Column.times(other: Any): Column = this.multiply(other)

/**
 * Kotlin `/` operator mapped to [Column.divide].
 *
 * Provided for symmetry with [times]; Spark's `$div` Scala operator is not accessible from Kotlin.
 */
operator fun Column.div(other: Any): Column = this.divide(other)

/**
 * Subscript operator for accessing a named struct field: `col["fieldName"]` → `col.getField("fieldName")`.
 */
operator fun Column.get(field: String): Column = this.getField(field)

/** Infix alias for [Column.equalTo]. Avoids confusion with Kotlin's `==` reference equality on [Column]. */
infix fun Column.eq(other: Any?): Column = this.equalTo(other)

/** Infix alias for [Column.notEqual]. */
infix fun Column.neq(other: Any?): Column = this.notEqual(other)

/** Infix alias for [Column.gt]. */
infix fun Column.gt(other: Any?): Column = this.gt(other)

/** Infix alias for [Column.geq] (greater-than-or-equal). */
infix fun Column.gte(other: Any?): Column = this.geq(other)

/** Infix alias for [Column.lt]. */
infix fun Column.lt(other: Any?): Column = this.lt(other)

/** Infix alias for [Column.leq] (less-than-or-equal). */
infix fun Column.lte(other: Any?): Column = this.leq(other)

/** Infix alias for [Column.and]. */
infix fun Column.and(other: Column): Column = this.and(other)

/** Infix alias for [Column.or]. */
infix fun Column.or(other: Column): Column = this.or(other)

// ============================================================================
// B) Dataset Extensions
// ============================================================================

/**
 * Prints the first [n] rows without truncating values. Delegates to [Dataset.show].
 */
fun <T> Dataset<T>.showPretty(n: Int = 20, truncate: Boolean = false) = this.show(n, truncate)

/**
 * Prints the first [n] rows and returns `this` for pipeline chaining.
 *
 * Useful for inspecting an intermediate [Dataset] without breaking a method chain.
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
 * Selects columns from this [Dataset] in a type-safe way using Kotlin property references.
 *
 * Property names are resolved at compile time, eliminating the risk of typos that would only
 * surface as a runtime [org.apache.spark.sql.AnalysisException].
 *
 * ```kotlin
 * val df: Dataset<Row> = people.toDataFrame(spark)
 * val names = df.selectTyped(Person::name, Person::age)
 * ```
 *
 * @param properties One or more property references from [T]. Each reference maps to a column
 *   of the same name in the DataFrame.
 * @return A new [Dataset]<[Row]> containing only the selected columns.
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

/**
 * Returns the value of column [name] cast to [T], or `null` if the field is null.
 *
 * Avoids a `NullPointerException` from [Row.getAs] on null fields for non-nullable [T].
 */
inline fun <reified T> Row.getNullable(name: String): T? =
    if (this.isNullAt(this.fieldIndex(name))) null else this.getAs<T>(name)
