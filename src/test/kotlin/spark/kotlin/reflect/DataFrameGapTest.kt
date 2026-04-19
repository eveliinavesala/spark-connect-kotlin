package spark.kotlin.reflect

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.reflect.typeOf
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

// ── Scalar types ───────────────────────────────────────────────────
internal data class WithShort(
    val x: Short,
)

internal data class WithByte(
    val x: Byte,
)

internal data class WithChar(
    val c: Char,
)

internal data class WithBigDecimal(
    val amount: BigDecimal,
)

internal data class WithByteArray(
    val blob: ByteArray,
)

internal data class WithArrayOfString(
    val tags: Array<String>,
)

// ── Transparent value class wrappers ───────────────────────────────
internal data class WithDuration(
    val timeout: Duration,
)

internal data class WithUInt(
    val id: UInt,
)

// ── Generic data class ─────────────────────────────────────────────
internal data class Box<T>(
    val value: T,
    val label: String,
)

internal data class ContainerWithGeneric(
    val box: Box<String>,
    val count: Int,
)

// ── Recursive type ─────────────────────────────────────────────────
internal data class Tree(
    val value: Int,
    val left: Tree?,
    val right: Tree?,
)

// ── Multi-level sealed hierarchy ───────────────────────────────────
internal sealed interface Shape

internal sealed interface Polygon : Shape

internal data class Circle(
    val radius: Double,
) : Shape

internal data class Triangle(
    val base: Double,
    val height: Double,
) : Polygon

internal data class Rectangle(
    val width: Double,
    val height: Double,
) : Polygon

class DataFrameGapTest : SparkTestBase() {
    // ── Scalar types — expected to fail today ──────────────────────

    @Test
    fun `Short round-trip`() {
        val result = listOf(WithShort(1), WithShort(2)).toDataFrame(spark).toKotlinList<WithShort>()
        assertEquals(listOf(WithShort(1), WithShort(2)), result)
    }

    @Test
    fun `Byte round-trip`() {
        val result = listOf(WithByte(1), WithByte(2)).toDataFrame(spark).toKotlinList<WithByte>()
        assertEquals(listOf(WithByte(1), WithByte(2)), result)
    }

    @Test
    fun `Char round-trip`() {
        val result = listOf(WithChar('A'), WithChar('z')).toDataFrame(spark).toKotlinList<WithChar>()
        assertEquals(listOf(WithChar('A'), WithChar('z')), result)
    }

    @Test
    fun `BigDecimal round-trip`() {
        val d1 = BigDecimal("123.456789")
        val result = listOf(WithBigDecimal(d1)).toDataFrame(spark).toKotlinList<WithBigDecimal>()
        assertEquals(0, d1.compareTo(result[0].amount))
    }

    @Test
    fun `ByteArray round-trip`() {
        val blob = byteArrayOf(1, 2, 3, 4)
        val result = listOf(WithByteArray(blob)).toDataFrame(spark).toKotlinList<WithByteArray>()
        assertArrayEquals(blob, result[0].blob)
    }

    @Test
    fun `Array of String round-trip`() {
        val result =
            listOf(WithArrayOfString(arrayOf("a", "b", "c"))).toDataFrame(spark).toKotlinList<WithArrayOfString>()
        assertArrayEquals(arrayOf("a", "b", "c"), result[0].tags)
    }

    // ── Value class wrappers — expected to work transparently ──────

    @Test
    fun `kotlin Duration round-trip`() {
        val result =
            listOf(WithDuration(5.seconds), WithDuration(10.seconds)).toDataFrame(spark).toKotlinList<WithDuration>()
        assertEquals(5.seconds, result[0].timeout)
        assertEquals(10.seconds, result[1].timeout)
    }

    @Test
    fun `UInt round-trip`() {
        val result = listOf(WithUInt(42u), WithUInt(0u)).toDataFrame(spark).toKotlinList<WithUInt>()
        assertEquals(42u, result[0].id)
        assertEquals(0u, result[1].id)
    }

    // ── Generic data class — top-level and nested ──────────────────

    @Test
    fun `generic data class Box-String round-trip`() {
        val input = listOf(Box("hello", "greeting"), Box("world", "noun"))
        val result = input.toDataFrame(spark).toKotlinList<Box<String>>()
        assertEquals(input, result)
    }

    @Test
    fun `nested generic data class field round-trip`() {
        val input =
            listOf(
                ContainerWithGeneric(Box("hello", "greeting"), 1),
                ContainerWithGeneric(Box("world", "noun"), 2),
            )
        val result = input.toDataFrame(spark).toKotlinList<ContainerWithGeneric>()
        assertEquals(input, result)
    }

    // ── Recursive type — should throw clearly, not stack overflow ──

    @Test
    fun `recursive type schema inference throws clearly`() {
        val ex =
            assertThrows(IllegalArgumentException::class.java) {
                getSparkSchema(typeOf<Tree>())
            }
        assertTrue(
            ex.message!!.contains("recursive", ignoreCase = true),
            "Expected message to mention 'recursive', was: ${ex.message}",
        )
    }

    // ── Multi-level sealed — expected to miss Polygon subclass fields

    @Test
    fun `multi-level sealed round-trip`() {
        val input: List<Shape> = listOf(Circle(1.0), Triangle(3.0, 4.0), Rectangle(5.0, 6.0))
        val result = input.toDataFrame(spark).toKotlinList<Shape>()
        assertEquals(input, result)
    }
}
