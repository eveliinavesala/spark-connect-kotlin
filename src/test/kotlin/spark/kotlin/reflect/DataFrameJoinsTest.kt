package spark.kotlin.reflect

import classes.SparkTestBase
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DataFrameJoinsTest : SparkTestBase() {
    data class Person(
        var name: String = "",
        var age: Int = 0,
        var cityId: Int = 0,
    )

    data class City(
        var id: Int = 0,
        var name: String = "",
    )

    data class JoinedResult(
        var personName: String = "",
        var cityName: String? = null,
    )

    private lateinit var peopleDF: Dataset<Row>
    private lateinit var citiesDF: Dataset<Row>

    @BeforeEach
    fun setup() {
        val peopleData =
            listOf(
                Person("Alice", 30, 1),
                Person("Bob", 40, 2),
                Person("Charlie", 25, 1),
                Person("David", 35, 3), // David lives in a city not in our cities list
            )
        val cityData =
            listOf(
                City(1, "Helsinki"),
                City(2, "Turku"),
                City(4, "Stockholm"), // Stockholm has no people
            )
        peopleDF = peopleData.toDataFrame(spark)
        citiesDF = cityData.toDataFrame(spark)
    }

    // --- 4. Joins and Set Operations ---

    @Test
    fun `join() should perform various types of joins`() {
        val people = peopleDF.alias("people")
        val cities = citiesDF.alias("cities")

        val innerJoin = people.join(cities, col("people.cityId").equalTo(col("cities.id")))
        assertEquals(3, innerJoin.count())

        val leftJoin =
            people
                .join(cities, col("people.cityId").equalTo(col("cities.id")), "left_outer")
                .select(col("people.name").`as`("personName"), col("cities.name").`as`("cityName"))

        val leftJoinList = leftJoin.toKotlinList<JoinedResult>()
        assertEquals(4, leftJoinList.size)
        val davidRow = leftJoinList.find { it.personName == "David" }
        assertNotNull(davidRow)
        assertNull(davidRow!!.cityName)
    }

    @Test
    fun `union() and unionByName() should combine datasets`() {
        val morePeople = listOf(Person("Eve", 28, 4)).toDataFrame(spark)

        val union = peopleDF.union(morePeople)
        assertEquals(5, union.count())

        val schema =
            StructType(
                arrayOf(
                    StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    StructField("cityId", DataTypes.IntegerType, false, Metadata.empty()),
                ),
            )
        val morePeopleDisordered = spark.createDataFrame(listOf(RowFactory.create(50, "Frank", 4)), schema)
        val unionByName = peopleDF.unionByName(morePeopleDisordered)

        val unionList = unionByName.toKotlinList<Person>()
        assertEquals(5, unionList.size)
        val frank = unionList.find { it.name == "Frank" }
        assertNotNull(frank)
        assertEquals(50, frank!!.age)
    }

    @Test
    fun `intersect() and except() should perform set operations`() {
        val subsetDF = listOf(Person("Alice", 30, 1), Person("Bob", 40, 2)).toDataFrame(spark)

        val intersection = peopleDF.intersect(subsetDF)
        assertEquals(2, intersection.count())

        val exception = peopleDF.except(subsetDF)
        val exceptionList = exception.toKotlinList<Person>()
        assertEquals(2, exceptionList.size)
        assertFalse(exceptionList.any { it.name == "Alice" })
    }
}
