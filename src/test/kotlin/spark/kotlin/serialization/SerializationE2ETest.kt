package spark.kotlin.serialization

import classes.SparkTestBase
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.size
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * End-to-End integration tests for kotlinx.serialization-based encoder/decoder.
 *
 * These tests verify the complete flow from Kotlin objects to DataFrames and back,
 * including DataFrame operations and transformations.
 */
class SerializationE2ETest : SparkTestBase() {
    @Test
    fun `test simple person DataFrame creation and collection`() {
        val people =
            listOf(
                SimplePerson("Alice", 30),
                SimplePerson("Bob", 25),
                SimplePerson("Charlie", 35),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show()

        // Verify schema
        assertEquals(2, df.columns().size)
        assertTrue(df.columns().contains("name"))
        assertTrue(df.columns().contains("age"))

        // Convert back to Kotlin
        val result = df.toSerializableKotlinList<SimplePerson>()

        assertEquals(3, result.size)
        assertEquals(people[0], result[0])
        assertEquals(people[1], result[1])
        assertEquals(people[2], result[2])
    }

    @Test
    fun `test DataFrame operations with serializable data`() {
        val people =
            listOf(
                SimplePerson("Alice", 30),
                SimplePerson("Bob", 25),
                SimplePerson("Charlie", 35),
                SimplePerson("David", 28),
            )

        val df = people.toSerializableDataFrame(spark)

        // Filter
        val filtered = df.filter(col("age").gt(27))
        val result = filtered.toSerializableKotlinList<SimplePerson>()

        assertEquals(3, result.size)
        assertTrue(result.all { it.age > 27 })
    }

    @Test
    fun `test sorting and limiting`() {
        val people =
            listOf(
                SimplePerson("Alice", 30),
                SimplePerson("Bob", 25),
                SimplePerson("Charlie", 35),
            )

        val df = people.toSerializableDataFrame(spark)

        // Sort by age descending and take top 2
        val sorted = df.orderBy(col("age").desc()).limit(2)
        val result = sorted.toSerializableKotlinList<SimplePerson>()

        assertEquals(2, result.size)
        assertEquals("Charlie", result[0].name) // age 35
        assertEquals("Alice", result[1].name) // age 30
    }

    @Test
    fun `test nested structures with DataFrame`() {
        val people =
            listOf(
                PersonWithAddress("Alice", 30, Address("123 Main St", "NYC", "10001")),
                PersonWithAddress("Bob", 25, Address("456 Oak Ave", "LA", "90001")),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        // Access nested field
        val cities = df.select(col("address.city"))
        cities.show()

        // Convert back
        val result = df.toSerializableKotlinList<PersonWithAddress>()

        assertEquals(2, result.size)
        assertEquals("NYC", result[0].address.city)
        assertEquals("LA", result[1].address.city)
    }

    @Test
    fun `test list fields with DataFrame operations`() {
        val people =
            listOf(
                PersonWithList("Alice", listOf("Pizza", "Pasta", "Salad")),
                PersonWithList("Bob", listOf("Burger", "Fries")),
                PersonWithList("Charlie", listOf()),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show(false)

        // Check array size
        val withSize = df.withColumn("foodCount", size(col("favoriteFoods")))
        withSize.show()

        // Filter by array size
        val hasMultipleFoods = df.filter(size(col("favoriteFoods")).gt(1))
        val result = hasMultipleFoods.toSerializableKotlinList<PersonWithList>()

        assertEquals(2, result.size)
        assertTrue(result.all { it.favoriteFoods.size > 1 })
    }

    @Test
    fun `test map fields with DataFrame`() {
        val people =
            listOf(
                PersonWithMap("Alice", mapOf("email" to "alice@example.com", "phone" to "555-1234")),
                PersonWithMap("Bob", mapOf("email" to "bob@example.com")),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        // Convert back
        val result = df.toSerializableKotlinList<PersonWithMap>()

        assertEquals(2, result.size)
        assertEquals("alice@example.com", result[0].attributes["email"])
        assertEquals(2, result[0].attributes.size)
        assertEquals(1, result[1].attributes.size)
    }

    @Test
    fun `test enum serialization`() {
        val people =
            listOf(
                PersonWithEnum("Alice", Status.ACTIVE),
                PersonWithEnum("Bob", Status.INACTIVE),
                PersonWithEnum("Charlie", Status.PENDING),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show()

        // Filter by enum value
        val active = df.filter(col("status").equalTo("ACTIVE"))
        val result = active.toSerializableKotlinList<PersonWithEnum>()

        assertEquals(1, result.size)
        assertEquals(Status.ACTIVE, result[0].status)
    }

    @Test
    fun `test sealed class polymorphism`() {
        val animals: List<Animal> =
            listOf(
                Dog("Buddy", "Golden Retriever"),
                Cat("Whiskers", "Orange"),
                Bird("Tweety", true),
            )

        val df = animals.toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        // Verify discriminator field exists
        assertTrue(df.columns().contains("_type"))

        // Convert back
        val result = df.toSerializableKotlinList<Animal>()

        assertEquals(3, result.size)
        assertTrue(result[0] is Dog)
        assertTrue(result[1] is Cat)
        assertTrue(result[2] is Bird)

        assertEquals("Buddy", result[0].name)
        assertEquals("Golden Retriever", (result[0] as Dog).breed)
    }

    @Test
    fun `test complex nested structure`() {
        val company =
            Company(
                name = "TechCorp",
                departments =
                    listOf(
                        Department(
                            name = "Engineering",
                            employees =
                                listOf(
                                    Employee("Alice", 100000.0, Address("123 Main", "NYC", "10001")),
                                    Employee("Bob", 95000.0, Address("456 Oak", "NYC", "10002")),
                                ),
                        ),
                        Department(
                            name = "Sales",
                            employees =
                                listOf(
                                    Employee("Charlie", 80000.0, Address("789 Elm", "LA", "90001")),
                                ),
                        ),
                    ),
            )

        val df = listOf(company).toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        // Convert back
        val result = df.toSerializableKotlinList<Company>()

        assertEquals(1, result.size)
        assertEquals("TechCorp", result[0].name)
        assertEquals(2, result[0].departments.size)
        assertEquals("Engineering", result[0].departments[0].name)
        assertEquals(2, result[0].departments[0].employees.size)
        assertEquals("Alice", result[0].departments[0].employees[0].name)
    }

    @Test
    fun `test nullable fields DataFrame operations`() {
        val people =
            listOf(
                NullableFields("Alice", 30, "alice@example.com"),
                NullableFields("Bob", null, null),
                NullableFields("Charlie", 35, null),
            )

        val df = people.toSerializableDataFrame(spark)
        df.show()

        // Filter non-null ages
        val withAge = df.filter(col("age").isNotNull)
        val result = withAge.toSerializableKotlinList<NullableFields>()

        assertEquals(2, result.size)
        assertNotNull(result[0].age)
        assertNotNull(result[1].age)
    }

    @Test
    fun `test aggregation operations`() {
        val people =
            listOf(
                SimplePerson("Alice", 30),
                SimplePerson("Bob", 25),
                SimplePerson("Charlie", 35),
                SimplePerson("David", 25),
            )

        val df = people.toSerializableDataFrame(spark)

        // Calculate average age
        val avgAge = df.agg(avg("age")).first().getDouble(0)
        assertEquals(28.75, avgAge, 0.01)

        // Count by age
        val counts = df.groupBy("age").count()
        counts.show()

        assertEquals(3, counts.count())
    }

    @Test
    fun `test empty list handling`() {
        val emptyList = emptyList<SimplePerson>()
        val df = emptyList.toSerializableDataFrame(spark)

        assertEquals(0, df.count())

        val result = df.toSerializableKotlinList<SimplePerson>()
        assertTrue(result.isEmpty())
    }

    @Test
    fun `test large dataset`() {
        // Create a larger dataset
        val people =
            (1..1000).map { i ->
                SimplePerson("Person$i", 20 + (i % 50))
            }

        val df = people.toSerializableDataFrame(spark)

        assertEquals(1000, df.count())

        // Perform some operations
        val over30 = df.filter(col("age").gt(30))
        val result = over30.toSerializableKotlinList<SimplePerson>()

        assertTrue(result.all { it.age > 30 })
    }

    @Test
    fun `test collection types with DataFrame`() {
        val data =
            listOf(
                CollectionTypes(
                    tags = listOf("kotlin", "spark"),
                    scores = mapOf("test1" to 95, "test2" to 88),
                    numbers = listOf(1, 2, 3),
                ),
                CollectionTypes(
                    tags = listOf("scala", "java"),
                    scores = mapOf("test1" to 92),
                    numbers = listOf(4, 5),
                ),
            )

        val df = data.toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        val result = df.toSerializableKotlinList<CollectionTypes>()

        assertEquals(2, result.size)
        assertEquals(listOf("kotlin", "spark"), result[0].tags)
        assertEquals(95, result[0].scores["test1"])
        assertEquals(listOf(1, 2, 3), result[0].numbers)
    }

    @Test
    fun `test datetime types serialization`() {
        val events =
            listOf(
                EventWithDates(
                    title = "Conference",
                    eventDate = LocalDate.parse("2024-12-01"),
                    timestamp = Instant.parse("2024-12-01T10:00:00Z"),
                ),
                EventWithDates(
                    title = "Workshop",
                    eventDate = LocalDate.parse("2024-12-15"),
                    timestamp = Instant.parse("2024-12-15T14:30:00Z"),
                ),
            )

        val df = events.toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        val result = df.toSerializableKotlinList<EventWithDates>()

        assertEquals(2, result.size)
        assertEquals("Conference", result[0].title)
        assertEquals(LocalDate.parse("2024-12-01"), result[0].eventDate)
        assertEquals(Instant.parse("2024-12-01T10:00:00Z"), result[0].timestamp)
    }

    @Test
    fun `test zoo with polymorphic animals`() {
        val zoo =
            Zoo(
                location = "Central Park Zoo",
                animals =
                    listOf(
                        Dog("Max", "Labrador"),
                        Cat("Luna", "Black"),
                        Bird("Rio", true),
                        Dog("Charlie", "Beagle"),
                    ),
            )

        val df = listOf(zoo).toSerializableDataFrame(spark)
        df.show(false)
        df.printSchema()

        val result = df.toSerializableKotlinList<Zoo>()

        assertEquals(1, result.size)
        assertEquals("Central Park Zoo", result[0].location)
        assertEquals(4, result[0].animals.size)

        val animals = result[0].animals
        assertTrue(animals[0] is Dog)
        assertTrue(animals[1] is Cat)
        assertTrue(animals[2] is Bird)
        assertTrue(animals[3] is Dog)

        assertEquals("Max", animals[0].name)
        assertEquals("Labrador", (animals[0] as Dog).breed)
    }

    @Test
    fun `test serialization cache effectiveness`() {
        val people1 = listOf(SimplePerson("Alice", 30))
        val people2 = listOf(SimplePerson("Bob", 25))

        // First call - should populate cache
        val df1 = people1.toSerializableDataFrame(spark)

        // Second call - should use cache
        val df2 = people2.toSerializableDataFrame(spark)

        // Both should work correctly
        assertEquals(1, df1.count())
        assertEquals(1, df2.count())

        val result1 = df1.toSerializableKotlinList<SimplePerson>()
        val result2 = df2.toSerializableKotlinList<SimplePerson>()

        assertEquals("Alice", result1[0].name)
        assertEquals("Bob", result2[0].name)
    }
}
