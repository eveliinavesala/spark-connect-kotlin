package typesafety

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import classes.SparkTestBase

class TypeSafetyTest : SparkTestBase() {

    /**
     * This test demonstrates the lack of compile-time type safety in the DataFrame API.
     * A simple typo in a column name ("ag" instead of "age") is not caught by the
     * compiler and only results in a runtime error when the job is executed.
     */
    @Test
    fun `dataframe api is not type-safe at compile time`() {
        val data = listOf(TypedPerson("Alice", 30))
        val df = spark.createDataFrame(data, TypedPerson::class.java)

        val query = df.select(functions.col("name"), functions.col("ag")) // Typo here

        // Use a robust try-catch to prove a runtime exception occurs.
        // The goal is just to prove an exception happens, not to check the message.
        try {
            query.collect()
            fail("Expected a runtime exception for the invalid column name, but none was thrown.")
        } catch (e: AnalysisException) {
            // Success! The expected exception was thrown at runtime.
        }
    }

    /**
     * This test demonstrates the compile-time type safety of the Dataset API,
     * but also the verbosity required to resolve ambiguity in the Java/Scala API.
     */
    @Test
    fun `dataset api is type-safe but verbose from kotlin`() {
        val data = listOf(TypedPerson("Alice", 30))
        
        // This test is expected to fail because of the bean encoder issue, which is part of our study.
        assertThrows(Exception::class.java) {
            val ds = spark.createDataset(data, Encoders.bean(TypedPerson::class.java))

            val mapFunction = MapFunction<TypedPerson, String> { person ->
                "Name: ${person.name}, Age: ${person.age}"
            }
            
            val result = ds.map(mapFunction, Encoders.STRING()).first()

            assertEquals("Name: Alice, Age: 30", result)
        }
    }
}
