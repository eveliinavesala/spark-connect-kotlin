package collections

import java.util.ArrayList

/**
 * A class representing the common Java pattern of using a mutable list implementation.
 * While Kotlin's `List` is read-only, Java code often uses concrete mutable types.
 */
class PersonWithMutableList(val name: String, val favoriteFoods: ArrayList<String>)
