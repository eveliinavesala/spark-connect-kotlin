package collections

/**
 * A data class containing a set of strings.
 * Sets enforce uniqueness of their elements.
 */
data class PersonWithSet(
    val name: String,
    val visitedCountries: Set<String>,
)
