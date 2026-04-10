package spark.kotlin.reflect

import classes.SparkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import java.time.Instant
import java.time.LocalDate
import kotlinx.datetime.LocalDate as KotlinxLocalDate
import kotlinx.datetime.Instant as KotlinxInstant

// internal (not private) — file-private types can't be reflected on from library code
internal sealed interface AppEvent
internal data class ButtonClick(val buttonId: Int) : AppEvent
internal data object AppReset : AppEvent

class DataFrameTypesTest : SparkTestBase() {

    data class Event(
        val eventId: Int,
        val eventDate: LocalDate,
        val eventTime: Instant
    )

    data class KotlinxEvent(
        val eventId: Int,
        val eventDate: KotlinxLocalDate,
        val eventTime: KotlinxInstant
    )

    @Test
    fun `should handle java time LocalDate and Instant`() {
        val today = LocalDate.now()
        val now = Instant.now()
        
        val data = listOf(
            Event(1, today, now),
            Event(2, today.minusDays(1), now.minusSeconds(3600))
        )
        
        val df = data.toDataFrame(spark)
        df.show()

        val results = df.toKotlinList<Event>()
        
        assertEquals(2, results.size)
        assertEquals(today, results[0].eventDate)
        // Spark's TimestampType may truncate sub-second precision; comparison is performed at second granularity
        assertEquals(now.epochSecond, results[0].eventTime.epochSecond)
    }

    @Test
    fun `should round-trip kotlinx datetime LocalDate and Instant`() {
        val data = listOf(
            KotlinxEvent(1, KotlinxLocalDate.parse("2024-06-15"), KotlinxInstant.parse("2024-06-15T10:00:00Z")),
            KotlinxEvent(2, KotlinxLocalDate.parse("2024-12-01"), KotlinxInstant.parse("2024-12-01T23:59:59Z"))
        )

        val results = data.toDataFrame(spark).toKotlinList<KotlinxEvent>()

        assertEquals(2, results.size)
        assertEquals(data[0].eventDate, results[0].eventDate)
        assertEquals(data[1].eventDate, results[1].eventDate)
        // Spark TimestampType has microsecond precision; compare at second granularity
        assertEquals(data[0].eventTime.epochSeconds, results[0].eventTime.epochSeconds)
        assertEquals(data[1].eventTime.epochSeconds, results[1].eventTime.epochSeconds)
    }

    @Test
    fun `should round-trip data object as sealed variant`() {
        val input: List<AppEvent> = listOf(ButtonClick(1), AppReset, ButtonClick(2), AppReset)
        val df = input.toDataFrame(spark)
        val result = df.toKotlinList<AppEvent>()
        assertEquals(input, result)
    }

    @Test
    fun `should serialize data object to row with only _type populated`() {
        val input: List<AppEvent> = listOf(AppReset)
        val df = input.toDataFrame(spark)
        val row = df.collectAsList().first()
        assertEquals("AppReset", row.getAs<String>("_type"))
        assertNull(row.getAs<Any?>("buttonId"))
    }
}
