package nullsafety

/**
 * A data class with a non-nullable property.
 * This is the idiomatic way to declare data that should always be present.
 * We expect this to fail at runtime if Spark tries to load null data into it.
 */
data class ItemWithNonNullable(val id: Int, val description: String)
