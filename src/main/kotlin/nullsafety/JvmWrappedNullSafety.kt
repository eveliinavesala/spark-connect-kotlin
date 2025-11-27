package nullsafety

/**
 * A data class with a nullable property.
 * This is the "Spark-safe" way to model a class that will be populated from a DataFrame,
 * as any column in a DataFrame can be null.
 */
data class ItemWithNullable(val id: Int, val description: String?)
