package spark.kotlin.benchmark

import classes.SparkTestBase
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import spark.kotlin.reflect.getSparkSchema
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import spark.kotlin.serialization.SerializationCache
import spark.kotlin.serialization.inferSparkSchema
import spark.kotlin.serialization.toSerializableDataFrame
import spark.kotlin.serialization.toSerializableKotlinList
import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import kotlin.reflect.typeOf
import kotlin.system.measureNanoTime

/**
 * Three-backend throughput benchmark using the Spotify Kaggle dataset.
 *
 * Backends compared:
 *   1. Reflection   — spark.kotlin.reflect (runtime KType introspection)
 *   2. Serialization — spark.kotlin.serialization (compile-time generated serializer)
 *   3. Raw DataFrame — spark.createDataFrame + collectAsList (Spark Connect wire baseline)
 *
 * ## JVM methodology
 *   - Blackhole pattern: result stored in volatile-like sink to prevent dead-code elimination
 *   - Warmup: WARMUP_ROUNDS full operations per backend before measurement
 *   - Measurement: MEASURE_ROUNDS iterations; median and min reported
 *   - GC: System.gc() + 200ms sleep before each backend's measurement block
 *   - nanoTime: used for all timing (not currentTimeMillis)
 *
 * ## Known limitation
 *   All backends run in the same JVM. JIT state from an earlier backend may influence later ones.
 *   The warmup phase mitigates but does not eliminate this. For isolated numbers, run each
 *   backend in a separate ./gradlew benchmark invocation.
 *
 * Run: ./gradlew test -Dtags=benchmark   (or a dedicated benchmark task)
 * Tags: @Tag("benchmark") — excluded from ./gradlew test by default.
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BackendBenchmarkTest : SparkTestBase() {

    companion object {
        private const val WARMUP_ROUNDS = 5
        private const val MEASURE_ROUNDS = 10
        private const val SCHEMA_WARMUP = 100_000
        private const val SCHEMA_MEASURE = 10
    }

    // ── Model ─────────────────────────────────────────────────────────────────

    @Serializable
    data class SpotifyTrack(
        val trackId: String,
        val trackName: String,
        val trackNumber: Int,
        val trackPopularity: Int,
        val explicit: Boolean,
        val artistName: String,
        val artistPopularity: Int,
        val artistFollowers: Long,
        val artistGenres: String,
        val albumId: String,
        val albumName: String,
        val albumReleaseDate: String,
        val albumTotalTracks: Int,
        val albumType: String,
        val durationMin: Double
    )

    // ── Data loading (excluded from timing) ───────────────────────────────────

    private lateinit var tracks: List<SpotifyTrack>
    private var nRows: Int = 0

    @BeforeAll
    fun loadData() {
        // SparkContainerManager copies the classpath "data/" dir to "/data" inside the container.
        // Use the container-side path; getResource().path returns a host path Spark cannot access.
        val csvPath = "/data/spotify_data_clean.csv"
        tracks = spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(csvPath)
            .collectAsList()
            .mapNotNull { row ->
                runCatching {
                    SpotifyTrack(
                        trackId          = row.getAs<String?>("track_id")           ?: return@mapNotNull null,
                        trackName        = row.getAs<String?>("track_name")         ?: "",
                        trackNumber      = toInt(row.getAs("track_number")),
                        trackPopularity  = toInt(row.getAs("track_popularity")),
                        explicit         = row.getAs<String?>("explicit")?.uppercase() == "TRUE",
                        artistName       = row.getAs<String?>("artist_name")        ?: "",
                        artistPopularity = toInt(row.getAs("artist_popularity")),
                        artistFollowers  = toLong(row.getAs("artist_followers")),
                        artistGenres     = row.getAs<String?>("artist_genres")      ?: "",
                        albumId          = row.getAs<String?>("album_id")           ?: "",
                        albumName        = row.getAs<String?>("album_name")         ?: "",
                        albumReleaseDate = row.getAs<String?>("album_release_date") ?: "",
                        albumTotalTracks = toInt(row.getAs("album_total_tracks")),
                        albumType        = row.getAs<String?>("album_type")         ?: "",
                        durationMin      = toDouble(row.getAs("track_duration_min"))
                    )
                }.getOrNull()
            }
        nRows = tracks.size
        println("\n[Benchmark] Loaded $nRows SpotifyTrack rows from CSV")
    }

    // ── Benchmarks ────────────────────────────────────────────────────────────

    @Test
    fun `round-trip throughput - reflection backend`() {
        warmupReflectSafe()
        val times = measure { tracks.toDataFrame(spark).toKotlinList<SpotifyTrack>() }
        report("round-trip", "reflect", times)
    }

    @Test
    fun `round-trip throughput - serialization backend`() {
        warmupSerializeSafe()
        val times = measure { tracks.toSerializableDataFrame(spark).toSerializableKotlinList<SpotifyTrack>() }
        report("round-trip", "serialize", times)
    }

    @Test
    fun `round-trip throughput - raw DataFrame baseline`() {
        // Pre-build GenericRowWithSchema objects so the only measured cost is
        // Spark Connect wire transfer + collect, with no library encode/decode overhead.
        val schema = getSparkSchema(typeOf<SpotifyTrack>())
        // Reflection schema sorts fields alphabetically. Values must match that order:
        // albumId, albumName, albumReleaseDate, albumTotalTracks, albumType,
        // artistFollowers, artistGenres, artistName, artistPopularity, durationMin,
        // explicit, trackId, trackName, trackNumber, trackPopularity
        val rawRows = tracks.map { t ->
            val values = arrayOf<Any?>(
                t.albumId, t.albumName, t.albumReleaseDate, t.albumTotalTracks, t.albumType,
                t.artistFollowers, t.artistGenres, t.artistName, t.artistPopularity, t.durationMin,
                t.explicit, t.trackId, t.trackName, t.trackNumber, t.trackPopularity
            )
            GenericRowWithSchema(values, schema)
        }
        // Warmup
        repeat(WARMUP_ROUNDS) {
            blackhole(spark.createDataFrame(rawRows, schema).collectAsList())
        }
        gc()
        val times = measure { spark.createDataFrame(rawRows, schema).collectAsList() }
        report("round-trip", "raw-baseline", times)
    }

    @Test
    fun `encode-only throughput - reflection backend`() {
        warmupReflectSafe()
        val times = measure { blackhole(tracks.toDataFrame(spark)) }
        report("encode-only", "reflect", times)
    }

    @Test
    fun `encode-only throughput - serialization backend`() {
        warmupSerializeSafe()
        val times = measure { blackhole(tracks.toSerializableDataFrame(spark)) }
        report("encode-only", "serialize", times)
    }

    @Test
    fun `decode-only throughput - reflection backend`() {
        val df = tracks.toDataFrame(spark)
        warmupReflectSafe()
        val times = measure { df.toKotlinList<SpotifyTrack>() }
        report("decode-only", "reflect", times)
    }

    @Test
    fun `decode-only throughput - serialization backend`() {
        val df = tracks.toSerializableDataFrame(spark)
        warmupSerializeSafe()
        val times = measure { df.toSerializableKotlinList<SpotifyTrack>() }
        report("decode-only", "serialize", times)
    }

    @Test
    fun `hardcoded schema - reflection backend`() {
        val schema = getSparkSchema(typeOf<SpotifyTrack>())
        warmupReflectSafe()
        val times = measure { tracks.toDataFrame(spark, schema).toKotlinList<SpotifyTrack>() }
        report("hardcoded-schema round-trip", "reflect+schema", times)
    }

    @Test
    fun `hardcoded schema - serialization backend`() {
        val schema = inferSparkSchema(serializer<SpotifyTrack>().descriptor)
        warmupSerializeSafe()
        val times = measure { tracks.toSerializableDataFrame(spark, schema).toSerializableKotlinList<SpotifyTrack>() }
        report("hardcoded-schema round-trip", "serialize+schema", times)
    }

    @Test
    fun `schema inference cost - reflection`() {
        // inferSparkSchema (internal) is not directly accessible; instead we compare:
        //   getSparkSchema — public, cached after first call
        //   First call in this test approximates cold (JIT-warm but cache-cold for SpotifyTrack).
        //   True cold requires a fresh JVM — this is documented as a known limitation.
        val kType = typeOf<SpotifyTrack>()

        // First call — cache miss (cold)
        val coldTime = measureNanoTime { blackhole(getSparkSchema(kType)) }

        // Warmup JIT on the cache-hit path
        repeat(SCHEMA_WARMUP) { blackhole(getSparkSchema(kType)) }

        // Warm — cache hits
        val warmTimes = (1..SCHEMA_MEASURE).map { measureNanoTime { blackhole(getSparkSchema(kType)) } }
        val warmMedian = median(warmTimes)

        println(buildString {
            appendLine("\n┌─ Schema inference — reflect")
            appendLine("│  cold (cache miss, first call): ${coldTime / 1_000}µs")
            appendLine("│  warm (cache hit, median of $SCHEMA_MEASURE): ${warmMedian}ns")
            appendLine("│  speedup: ${coldTime / warmMedian.coerceAtLeast(1)}x")
            append("└─")
        })
        appendCsv("schema-cold", "reflect", listOf(coldTime), 1)
        appendCsv("schema-warm", "reflect", warmTimes, 1)
    }

    @Test
    fun `schema inference cost - serialization`() {
        // inferSparkSchema always recomputes — measures raw descriptor walk cost without the cache.
        val descriptor = serializer<SpotifyTrack>().descriptor

        // Warmup JIT
        repeat(SCHEMA_WARMUP) { blackhole(inferSparkSchema(descriptor)) }

        val recomputeTimes = (1..SCHEMA_MEASURE).map { measureNanoTime { blackhole(inferSparkSchema(descriptor)) } }

        println(buildString {
            appendLine("\n┌─ Schema inference — serialize (inferSparkSchema, always recomputes)")
            appendLine("│  median of $SCHEMA_MEASURE: ${median(recomputeTimes) / 1_000}µs")
            appendLine("│  min: ${recomputeTimes.min() / 1_000}µs")
            append("└─")
        })
        appendCsv("schema-recompute", "serialize", recomputeTimes, 1)
    }

    @Test
    fun `schema inference cost - serialization cache`() {
        // SerializationCache.getSchema() is the call path used by toSerializableDataFrame.
        // Clears the cache to get a true cold measurement; other tests that run after this
        // will repopulate the cache on their first warmup round.
        val ser = serializer<SpotifyTrack>()

        SerializationCache.clearAll()
        val coldTime = measureNanoTime { blackhole(SerializationCache.getSchema(ser)) }

        // Warmup JIT on the cache-hit path
        repeat(SCHEMA_WARMUP) { blackhole(SerializationCache.getSchema(ser)) }

        val warmTimes = (1..SCHEMA_MEASURE).map { measureNanoTime { blackhole(SerializationCache.getSchema(ser)) } }
        val warmMedian = median(warmTimes)

        println(buildString {
            appendLine("\n┌─ Schema inference — serialize (SerializationCache)")
            appendLine("│  cold (cache miss, first call): ${coldTime / 1_000}µs")
            appendLine("│  warm (cache hit, median of $SCHEMA_MEASURE): ${warmMedian}ns")
            appendLine("│  speedup: ${coldTime / warmMedian.coerceAtLeast(1)}x")
            append("└─")
        })
        appendCsv("schema-cold", "serialize", listOf(coldTime), 1)
        appendCsv("schema-warm", "serialize", warmTimes, 1)
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    @Suppress("UNUSED_PARAMETER")
    private fun blackhole(value: Any?) { /* intentionally empty — parameter prevents DCE */ }

    private fun warmupReflect() {
        repeat(WARMUP_ROUNDS) { blackhole(tracks.toDataFrame(spark).toKotlinList<SpotifyTrack>()) }
        gc()
    }

    private fun warmupSerialize() {
        repeat(WARMUP_ROUNDS) { blackhole(tracks.toSerializableDataFrame(spark).toSerializableKotlinList<SpotifyTrack>()) }
        gc()
    }

    private fun gc() {
        System.gc()
        Thread.sleep(200)
    }

    // Retries a single Spark Connect call on transient gRPC UNAVAILABLE errors
    // (connection closed mid-stream). The retry attempts are not included in any
    // timing measurement — only the successful call is timed.
    private fun <T> retryOnUnavailable(maxAttempts: Int = 3, block: () -> T): T {
        var lastException: Exception? = null
        repeat(maxAttempts) { attempt ->
            try {
                return block()
            } catch (e: Exception) {
                if ("UNAVAILABLE" in (e.message ?: "") || "Network closed" in (e.message ?: "")) {
                    lastException = e
                    Thread.sleep(500L * (attempt + 1))
                } else {
                    throw e
                }
            }
        }
        throw lastException!!
    }

    private fun measure(block: () -> Any?): List<Long> {
        return (1..MEASURE_ROUNDS).map { measureNanoTime { blackhole(retryOnUnavailable { block() }) } }
    }

    private fun warmupReflectSafe() {
        repeat(WARMUP_ROUNDS) { retryOnUnavailable { blackhole(tracks.toDataFrame(spark).toKotlinList<SpotifyTrack>()) } }
        gc()
    }

    private fun warmupSerializeSafe() {
        repeat(WARMUP_ROUNDS) { retryOnUnavailable { blackhole(tracks.toSerializableDataFrame(spark).toSerializableKotlinList<SpotifyTrack>()) } }
        gc()
    }

    private fun median(times: List<Long>): Long {
        val sorted = times.sorted()
        return if (sorted.size % 2 == 0)
            (sorted[sorted.size / 2 - 1] + sorted[sorted.size / 2]) / 2
        else
            sorted[sorted.size / 2]
    }

    private fun report(workload: String, backend: String, timesNs: List<Long>) {
        val medianMs  = median(timesNs) / 1_000_000.0
        val minMs     = timesNs.min() / 1_000_000.0
        val maxMs     = timesNs.max() / 1_000_000.0
        val rowsPerSec = (nRows / (medianMs / 1000.0)).toLong()

        println(buildString {
            appendLine("\n┌─ $workload  [$backend]  n=$nRows rows  ($MEASURE_ROUNDS measurements)")
            appendLine("│  median : ${String.format(Locale.US, "%8.1f", medianMs)} ms   →  $rowsPerSec rows/s")
            appendLine("│  min    : ${String.format(Locale.US, "%8.1f", minMs)} ms")
            appendLine("│  max    : ${String.format(Locale.US, "%8.1f", maxMs)} ms")
            append("└─")
        })

        appendCsv(workload, backend, timesNs, nRows)
    }

    private fun appendCsv(workload: String, backend: String, timesNs: List<Long>, n: Int) {
        val outDir = File("test-results/benchmarking-results").also { it.mkdirs() }
        val ts = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneOffset.UTC).format(Instant.now())
        val file = File(outDir, "backend-benchmark-$ts.csv")
        if (!file.exists()) {
            file.writeText("timestamp,backend,workload,n_rows,warmup_rounds,measure_rounds,median_ms,min_ms,max_ms,rows_per_sec_median\n")
        }
        val medianMs  = median(timesNs) / 1_000_000.0
        val minMs     = timesNs.min() / 1_000_000.0
        val maxMs     = timesNs.max() / 1_000_000.0
        val rowsPerSec = (n / (medianMs / 1000.0)).toLong()
        file.appendText("$ts,$backend,$workload,$n,$WARMUP_ROUNDS,$MEASURE_ROUNDS," +
                "${String.format(Locale.US, "%.1f", medianMs)},${String.format(Locale.US, "%.1f", minMs)},${String.format(Locale.US, "%.1f", maxMs)},$rowsPerSec\n")
    }

    // ── Type coercion helpers for CSV rows ────────────────────────────────────
    // Spark's CSV inferSchema may return Int, Long, Double or String depending on the column.

    private fun toInt(v: Any?): Int = when (v) {
        is Int    -> v
        is Long   -> v.toInt()
        is Double -> v.toInt()
        is String -> v.toIntOrNull() ?: 0
        else      -> 0
    }

    private fun toLong(v: Any?): Long = when (v) {
        is Long   -> v
        is Int    -> v.toLong()
        is Double -> v.toLong()
        is String -> v.toLongOrNull() ?: 0L
        else      -> 0L
    }

    private fun toDouble(v: Any?): Double = when (v) {
        is Double -> v
        is Float  -> v.toDouble()
        is Int    -> v.toDouble()
        is Long   -> v.toDouble()
        is String -> v.toDoubleOrNull() ?: 0.0
        else      -> 0.0
    }
}
