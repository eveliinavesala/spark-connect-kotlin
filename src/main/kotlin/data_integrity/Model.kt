package data_integrity

/**
 * A simple data class to demonstrate typed operations in Spark.
 * Used to test the compile-time safety of the Dataset API vs. DataFrame.
 */
data class TypedPerson(val name: String, val age: Int)

/**
 * A data class with a non-nullable property.
 * This is the idiomatic way to declare data that should always be present.
 * We expect this to fail at runtime if Spark tries to load null data into it.
 */
data class ItemWithNonNullable(val id: Int, val description: String)

/**
 * A data class with a nullable property.
 * This is the "Spark-safe" way to model a class that will be populated from a DataFrame,
 * as any column in a DataFrame can be null.
 */
data class ItemWithNullable(val id: Int, val description: String?)
