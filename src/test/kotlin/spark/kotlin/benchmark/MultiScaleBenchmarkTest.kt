package spark.kotlin.benchmark

import classes.SparkTestBase
import kotlinx.serialization.Serializable
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import spark.kotlin.reflect.toDataFrame
import spark.kotlin.reflect.toKotlinList
import spark.kotlin.serialization.toSerializableDataFrame
import spark.kotlin.serialization.toSerializableKotlinList
import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import kotlin.system.measureNanoTime

/**
 * Multi-scale throughput benchmark: runs encode-only, decode-only, and round-trip
 * across the three dataset sizes (8k, 50k, 100k) to establish scalability curves.
 *
 * Run: ./gradlew benchmark
 * Results: build/benchmark-results/multi-scale-benchmark-<ts>.csv
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiScaleBenchmarkTest : SparkTestBase() {

    companion object {
        private const val WARMUP_ROUNDS = 3
        private const val MEASURE_ROUNDS = 7

        private val DATASETS = listOf(
            "8k"   to "/data/spotify_data_clean.csv",
            "50k"  to "/data/spotify_50k.csv",
            // 100k omitted: requires ≥8 GB available RAM; causes Spark container OOM on constrained machines.
            // Uncomment when running on a machine with ≥8 GB free:
            // "100k" to "/data/spotify_100k.csv",
        )
    }

    // ── Model (same as BackendBenchmarkTest) ──────────────────────────────────

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

    // ── Helpers ───────────────────────────────────────────────────────────────

    @Suppress("UNUSED_PARAMETER")
    private fun blackhole(value: Any?) {}

    private fun gc() { System.gc(); Thread.sleep(200) }

    private fun measure(block: () -> Any?): List<Long> =
        (1..MEASURE_ROUNDS).map { measureNanoTime { blackhole(block()) } }

    private fun median(times: List<Long>): Long {
        val s = times.sorted()
        return if (s.size % 2 == 0) (s[s.size / 2 - 1] + s[s.size / 2]) / 2 else s[s.size / 2]
    }

    private fun loadTracks(csvPath: String): List<SpotifyTrack> =
        spark.read()
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

    private fun report(workload: String, backend: String, timesNs: List<Long>, nRows: Int, sizeLabel: String) {
        val medianMs   = median(timesNs) / 1_000_000.0
        val minMs      = timesNs.min() / 1_000_000.0
        val maxMs      = timesNs.max() / 1_000_000.0
        val rowsPerSec = (nRows / (medianMs / 1000.0)).toLong()

        println(buildString {
            appendLine("\n┌─ $workload  [$backend]  scale=$sizeLabel  n=$nRows  ($MEASURE_ROUNDS measurements)")
            appendLine("│  median : ${String.format(Locale.US, "%8.1f", medianMs)} ms   →  $rowsPerSec rows/s")
            appendLine("│  min    : ${String.format(Locale.US, "%8.1f", minMs)} ms")
            appendLine("│  max    : ${String.format(Locale.US, "%8.1f", maxMs)} ms")
            append("└─")
        })

        val outDir = File("build/benchmark-results").also { it.mkdirs() }
        val ts = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").withZone(ZoneOffset.UTC).format(Instant.now())
        val file = File(outDir, "multi-scale-benchmark-$ts.csv")
        if (!file.exists()) {
            file.writeText("timestamp,scale,backend,workload,n_rows,warmup_rounds,measure_rounds,median_ms,min_ms,max_ms,rows_per_sec_median\n")
        }
        file.appendText(
            "$ts,$sizeLabel,$backend,$workload,$nRows,$WARMUP_ROUNDS,$MEASURE_ROUNDS," +
            "${String.format(Locale.US, "%.1f", medianMs)},${String.format(Locale.US, "%.1f", minMs)},${String.format(Locale.US, "%.1f", maxMs)},$rowsPerSec\n"
        )
    }

    // ── Benchmark tests ───────────────────────────────────────────────────────

    @Test
    fun `round-trip - reflection - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            println("\n[MultiScale] Loaded ${tracks.size} rows from $csvPath")
            repeat(WARMUP_ROUNDS) { blackhole(tracks.toDataFrame(spark).toKotlinList<SpotifyTrack>()) }
            gc()
            val times = measure { tracks.toDataFrame(spark).toKotlinList<SpotifyTrack>() }
            report("round-trip", "reflect", times, tracks.size, label)
        }
    }

    @Test
    fun `round-trip - serialization - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            println("\n[MultiScale] Loaded ${tracks.size} rows from $csvPath")
            repeat(WARMUP_ROUNDS) { blackhole(tracks.toSerializableDataFrame(spark).toSerializableKotlinList<SpotifyTrack>()) }
            gc()
            val times = measure { tracks.toSerializableDataFrame(spark).toSerializableKotlinList<SpotifyTrack>() }
            report("round-trip", "serialize", times, tracks.size, label)
        }
    }

    @Test
    fun `decode-only - reflection - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            val df = tracks.toDataFrame(spark)
            repeat(WARMUP_ROUNDS) { blackhole(df.toKotlinList<SpotifyTrack>()) }
            gc()
            val times = measure { df.toKotlinList<SpotifyTrack>() }
            report("decode-only", "reflect", times, tracks.size, label)
        }
    }

    @Test
    fun `decode-only - serialization - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            val df = tracks.toSerializableDataFrame(spark)
            repeat(WARMUP_ROUNDS) { blackhole(df.toSerializableKotlinList<SpotifyTrack>()) }
            gc()
            val times = measure { df.toSerializableKotlinList<SpotifyTrack>() }
            report("decode-only", "serialize", times, tracks.size, label)
        }
    }

    @Test
    fun `encode-only - reflection - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            repeat(WARMUP_ROUNDS) { blackhole(tracks.toDataFrame(spark)) }
            gc()
            val times = measure { blackhole(tracks.toDataFrame(spark)) }
            report("encode-only", "reflect", times, tracks.size, label)
        }
    }

    @Test
    fun `encode-only - serialization - all scales`() {
        for ((label, csvPath) in DATASETS) {
            val tracks = loadTracks(csvPath)
            repeat(WARMUP_ROUNDS) { blackhole(tracks.toSerializableDataFrame(spark)) }
            gc()
            val times = measure { blackhole(tracks.toSerializableDataFrame(spark)) }
            report("encode-only", "serialize", times, tracks.size, label)
        }
    }

    // ── Type coercion helpers ─────────────────────────────────────────────────

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
