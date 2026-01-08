package dataframe

import classes.SparkTestBase
import integration_pack.toDataFrame
import integration_pack.toKotlinList
import kotlinx.datetime.Clock
import kotlinx.datetime.TimeZone
import kotlinx.datetime.todayIn
import org.apache.spark.sql.RowFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

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
        // Note: Spark's TimestampType might truncate nanoseconds, so we compare seconds
        assertEquals(now.epochSecond, results[0].eventTime.epochSecond)
    }

    @Test
    fun `should handle kotlinx datetime types`() {
        // We need to convert kotlinx types to java.sql types for Spark
        val today = Clock.System.todayIn(TimeZone.UTC)
        val now = Clock.System.now()

        val data = listOf(
            mapOf(
                "eventId" to 1,
                "eventDate" to java.sql.Date.valueOf(today.toString()),
                "eventTime" to Timestamp.from(java.time.Instant.parse(now.toString()))
            )
        )
        
        // Use RowFactory.create instead of Row.fromSeq to avoid Scala interop issues
        val rows = data.map { 
            RowFactory.create(it["eventId"], it["eventDate"], it["eventTime"]) 
        }
        
        val df = spark.createDataFrame(rows,
            org.apache.spark.sql.types.StructType()
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
}
