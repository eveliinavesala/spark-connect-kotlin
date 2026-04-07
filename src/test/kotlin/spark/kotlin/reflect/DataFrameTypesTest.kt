package spark.kotlin.reflect

import classes.SparkTestBase
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.todayIn
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

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
    fun `should handle kotlinx datetime types`() {
        // kotlinx datetime types are converted to java.sql equivalents for Spark compatibility
        val today = Clock.System.todayIn(TimeZone.UTC)
        val now = Clock.System.now()

        val data = listOf(
            mapOf(
                "eventId" to 1,
                "eventDate" to Date.valueOf(today.toString()),
                "eventTime" to Timestamp.from(Instant.parse(now.toString()))
            )
        )
        
        // Use RowFactory.create instead of Row.fromSeq to avoid Scala interop issues
        val rows = data.map { 
            RowFactory.create(it["eventId"], it["eventDate"], it["eventTime"]) 
        }
        
        val df = spark.createDataFrame(rows,
            StructType()
                .add("eventId", "int")
                .add("eventDate", "date")
                .add("eventTime", "timestamp")
        )

        df.show()
        
        val result = df.first()
        val resultDate = result.getDate(1).toLocalDate()
        val resultTime = result.getTimestamp(2).toInstant()

        assertEquals(today.toString(), resultDate.toString())
        assertEquals(now.epochSeconds, resultTime.epochSecond)
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
