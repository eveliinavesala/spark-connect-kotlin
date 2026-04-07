package spark.kotlin.reflect

import classes.SparkTestBase
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.reflect.typeOf

// Two nested subclasses of the same sealed parent that share the same simpleName ("Child").
// sealedSubclasses resolves to both, causing the duplicate-detection check to throw.
private sealed class DuplicateSealedParent
private class ContainerA { class Child : DuplicateSealedParent() }
private class ContainerB { class Child : DuplicateSealedParent() }

private data class WithUnsupportedField(val thread: Thread)

private class NoPrimaryConstructor {
    val name: String
    constructor(name: String) { this.name = name }
}

class ErrorCasesTest : SparkTestBase() {

    @Test
    fun `getSparkSchema should throw for sealed class with duplicate subclass simpleName`() {
        val ex = assertThrows<IllegalArgumentException> {
            getSparkSchema(typeOf<DuplicateSealedParent>())
        }
        assertTrue(
            ex.message!!.contains("duplicate", ignoreCase = true),
            "Error message should mention duplicates, was: ${ex.message}"
        )
    }

    @Test
    fun `getSparkSchema should throw for unsupported type and include type name in message`() {
        val ex = assertThrows<IllegalArgumentException> {
            getSparkSchema(typeOf<WithUnsupportedField>())
        }
        assertTrue(
            ex.message!!.contains("Thread"),
            "Error message should contain the unsupported type name 'Thread', was: ${ex.message}"
        )
    }

    @Test
    fun `toKotlinList should throw for class with no primary constructor`() {
        val schema = StructType(arrayOf(
            StructField("name", DataTypes.StringType, false, Metadata.empty())
        ))
        val df = spark.createDataFrame(listOf(RowFactory.create("test")), schema)
        val ex = assertThrows<IllegalStateException> {
            df.toKotlinList<NoPrimaryConstructor>()
        }
        assertTrue(
            ex.message!!.contains("primary constructor", ignoreCase = true),
            "Error message should mention primary constructor, was: ${ex.message}"
        )
    }
}
