package collections

/**
 * A data class containing a map of strings to integers.
 * This is useful for storing properties or attributes.
 */
data class UserWithScores(
    val id: String,
    val scores: Map<String, Int>,
)
