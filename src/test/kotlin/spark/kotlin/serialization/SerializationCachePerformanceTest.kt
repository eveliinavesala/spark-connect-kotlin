package spark.kotlin.serialization

import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import spark.kotlin.benchmark.ExecutionIdGenerator
import spark.kotlin.benchmark.PerformanceDataPoint
import spark.kotlin.benchmark.PerformanceDataWriter
import spark.kotlin.benchmark.SystemInfo
import spark.kotlin.benchmark.SystemInfoCollector
import spark.kotlin.benchmark.TestMetadata
import spark.kotlin.benchmark.getCurrentTimestamp
import java.io.File
import java.util.Locale
import kotlin.system.measureNanoTime
import kotlin.test.assertTrue

/**
 * Performance tests for [SerializationCache] verifying O(1) complexity of cached lookups.
 *
 * Mirrors [spark.kotlin.reflect.ReflectionCacheTest] for structural parity.
 * [SerializationCache] is keyed by [kotlinx.serialization.descriptors.SerialDescriptor]
 * (accessed via [kotlinx.serialization.KSerializer]) rather than by [kotlin.reflect.KType].
 *
 * Tests 1–2: measurement method validity and lookup stability.
 * Tests 3–5: see [SerializationCacheStabilityTest].
 *
 * Output CSVs are written to `test-results/performance-serialization/`.
 */
class SerializationCachePerformanceTest {
    @Serializable
    data class ComplexType(
        val id: Int,
        val name: String,
        val description: String,
        val value: Double,
        val active: Boolean,
        val tags: List<String>,
        val metadata: Map<String, Int>,
    )

    @Suppress("UNUSED_PARAMETER")
    private fun blackhole(value: Any?) { // prevents dead-code elimination
    }

    // ── Test 1: Measurement validation ───────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("Measurement validation: Compare known O(1) vs O(n)")
    fun `measurement method correctly distinguishes O(1) from O(n)`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints = mutableListOf<PerformanceDataPoint>()
        var testResult = "PASSED"
        var testStatus = "completed"
        var failureReason = ""
        val warmupIterations = 10_000

        try {
            println("\n=== Test 1: Measurement Validation ===")
            println("Purpose: Verify our benchmarking methodology works correctly")
            println("Expected: HashMap stays constant, ArrayList grows linearly\n")

            val sampleSizes = listOf(10_000, 50_000, 100_000)

            println("HashMap (O(1)) lookups:")
            val hashMapTimes =
                sampleSizes.map { size ->
                    val si = SystemInfoCollector.collect()
                    val (date, time) = getCurrentTimestamp()
                    val map = (1..size).associateBy { it }
                    val target = size / 2
                    var result: Int? = null
                    val timeNs = measureNanoTime { repeat(10_000) { result = map[target] } } / 10_000
                    blackhole(result)
                    println("  n=$size: ${timeNs}ns")
                    dataPoints +=
                        dp(
                            "test1_measurement_validation",
                            "test1",
                            executionId,
                            date,
                            time,
                            size,
                            1,
                            timeNs,
                            0,
                            si,
                            testResult,
                            testStatus,
                            failureReason,
                            mapOf(
                                "test1_data_structure" to "HashMap",
                                "test1_expected_complexity" to "O(1)",
                            ),
                        )
                    size to timeNs
                }

            println("\nArrayList.contains (O(n)) lookups:")
            val arrayListTimes =
                sampleSizes.map { size ->
                    val si = SystemInfoCollector.collect()
                    val (date, time) = getCurrentTimestamp()
                    val list = (1..size).toList()
                    var result = false
                    val timeNs = measureNanoTime { repeat(1_000) { result = list.contains(size) } } / 1_000
                    blackhole(result)
                    println("  n=$size: ${timeNs}ns")
                    dataPoints +=
                        dp(
                            "test1_measurement_validation",
                            "test1",
                            executionId,
                            date,
                            time,
                            size,
                            1,
                            timeNs,
                            0,
                            si,
                            testResult,
                            testStatus,
                            failureReason,
                            mapOf(
                                "test1_data_structure" to "ArrayList",
                                "test1_expected_complexity" to "O(n)",
                            ),
                        )
                    size to timeNs
                }

            val hashMapGrowth = hashMapTimes.last().second.toDouble() / hashMapTimes.first().second
            val arrayListGrowth = arrayListTimes.last().second.toDouble() / arrayListTimes.first().second
            println("\nHashMap growth (10k→100k): ${String.format(Locale.ROOT, "%.2f", hashMapGrowth)}x")
            println("ArrayList growth (10k→100k): ${String.format(Locale.ROOT, "%.2f", arrayListGrowth)}x")

            dataPoints.filter { it.metrics["test1_data_structure"] == "HashMap" }.forEach { point ->
                val index = dataPoints.indexOf(point)
                dataPoints[index] =
                    point.copy(
                        metrics =
                            point.metrics +
                                mapOf(
                                    "test1_structure_growth_10k_100k" to
                                        String.format(Locale.ROOT, "%.2f", hashMapGrowth),
                                ),
                    )
            }

            dataPoints.filter { it.metrics["test1_data_structure"] == "ArrayList" }.forEach { point ->
                val index = dataPoints.indexOf(point)
                dataPoints[index] =
                    point.copy(
                        metrics =
                            point.metrics +
                                mapOf(
                                    "test1_structure_growth_10k_100k" to
                                        String.format(Locale.ROOT, "%.2f", arrayListGrowth),
                                ),
                    )
            }

            assertTrue(
                hashMapGrowth < 3.0,
                "HashMap should be O(1), growth was ${String.format(Locale.ROOT, "%.2f", hashMapGrowth)}x",
            )
            assertTrue(
                arrayListGrowth > 2.0,
                "ArrayList.contains should be O(n), growth was ${String.format(Locale.ROOT, "%.2f", arrayListGrowth)}x",
            )
            println("✓ Measurement method valid: correctly identifies O(1) vs O(n)")

            println("\nSerializationCache.getSchema:")
            val ser = serializer<ComplexType>()
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }
            val si = SystemInfoCollector.collect()
            val (date, time) = getCurrentTimestamp()
            var cacheResult: StructType? = null
            val cacheTime =
                measureNanoTime { repeat(10_000) { cacheResult = SerializationCache.getSchema(ser) } } / 10_000
            blackhole(cacheResult)
            println("  Cache lookup: ${cacheTime}ns")
            dataPoints +=
                dp(
                    "test1_measurement_validation",
                    "test1",
                    executionId,
                    date,
                    time,
                    10_000,
                    1,
                    cacheTime,
                    warmupIterations,
                    si,
                    testResult,
                    testStatus,
                    failureReason,
                    mapOf(
                        "test1_data_structure" to "SerializationCache",
                        "test1_expected_complexity" to "O(1)",
                        "test1_structure_growth_10k_100k" to "N/A",
                    ),
                )
            val cacheVsHashMap = cacheTime.toDouble() / hashMapTimes.first().second
            assertTrue(
                cacheVsHashMap < 100.0,
                "Cache should have O(1) characteristics, was ${
                    String.format(Locale.ROOT, "%.1f", cacheVsHashMap)
                }x slower than HashMap",
            )
            println("✓ SerializationCache exhibits O(1) characteristics")
        } catch (e: AssertionError) {
            testResult = "FAILED"
            testStatus = "failed_assertion"
            failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll {
                it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason)
            }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"
            testStatus = "failed_error"
            failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll {
                it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason)
            }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) {
                PerformanceDataWriter(File("test-results/performance-serialization")).write(
                    "test1_measurement_validation",
                    meta(
                        "Test 1 - Measurement Validation",
                        "Verify benchmarking methodology correctly distinguishes O(1) from O(n)",
                        "measurement method correctly distinguishes O(1) from O(n)()",
                        warmupIterations,
                        listOf(10_000, 50_000, 100_000),
                        1,
                        false,
                        0,
                        "HashMap growth < 3.0x, ArrayList growth > 2.0x",
                    ),
                    dataPoints,
                )
            }
        }
    }

    // ── Test 2: Lookup stability ──────────────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) complexity with proper JVM warmup")
    fun `cache lookup is O(1) with JVM warmup`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints = mutableListOf<PerformanceDataPoint>()
        var testResult = "PASSED"
        var testStatus = "completed"
        var failureReason = ""
        val warmupIterations = 300_000
        val batchSize = 10_000
        val numSamples = 50

        try {
            println("\n=== Test 2: Cache Lookup Stability (Percentile-based) ===")
            println("Purpose: Verify cache hit time is stable — not growing with call count")
            println("Method:  $numSamples samples × $batchSize ops, fixed batch size eliminates JIT loop variance")
            println("Expected: P90/P10 spread < 20x, median < 10µs\n")

            val ser = serializer<ComplexType>()
            println("Warming up JVM ($warmupIterations iterations)...")
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }
            System.gc()
            Thread.sleep(200)

            println("Collecting $numSamples samples...")
            val samplesNs =
                LongArray(numSamples) { sampleIdx ->
                    val si = SystemInfoCollector.collect()
                    val (date, time) = getCurrentTimestamp()
                    var result: StructType? = null
                    val total = measureNanoTime { repeat(batchSize) { result = SerializationCache.getSchema(ser) } }
                    blackhole(result)
                    val perOpNs = total / batchSize
                    dataPoints +=
                        dp(
                            "test2_constant_time_growth",
                            "test2",
                            executionId,
                            date,
                            time,
                            batchSize,
                            sampleIdx + 1,
                            perOpNs,
                            warmupIterations,
                            si,
                            testResult,
                            testStatus,
                            failureReason,
                            emptyMap(),
                        )
                    perOpNs
                }

            samplesNs.sort()
            val p10 = samplesNs[numSamples / 10]
            val p50 = samplesNs[numSamples / 2]
            val p90 = samplesNs[numSamples * 9 / 10]
            val p99 = samplesNs[(numSamples * 99 / 100).coerceAtMost(numSamples - 1)]
            val spread = p90.toDouble() / p10.coerceAtLeast(1L)
            println("p10=${p10}ns  p50=${p50}ns  p90=${p90}ns  p99=${p99}ns")
            println("P90/P10 spread: ${String.format(Locale.ROOT, "%.1f", spread)}x")

            val metrics =
                mapOf(
                    "test2_p10_ns" to p10.toString(),
                    "test2_p50_ns" to p50.toString(),
                    "test2_p90_ns" to p90.toString(),
                    "test2_p99_ns" to p99.toString(),
                    "test2_spread_p90_p10" to String.format(Locale.ROOT, "%.2f", spread),
                    "test2_spread_threshold" to "20.0",
                    "test2_p50_threshold_ns" to "10000",
                )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            assertTrue(
                spread < 20.0,
                "P90/P10 spread ${String.format(Locale.ROOT, "%.1f", spread)}x exceeds 20x — lookup is not stable",
            )
            assertTrue(
                p50 < 10_000L,
                "Median lookup ${p50}ns exceeds 10µs — cache may be recomputing on every call",
            )
            println("✓ O(1) confirmed: spread=${String.format(Locale.ROOT, "%.1f", spread)}x, median=${p50}ns")
        } catch (e: AssertionError) {
            testResult = "FAILED"
            testStatus = "failed_assertion"
            failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll {
                it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason)
            }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"
            testStatus = "failed_error"
            failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll {
                it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason)
            }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) {
                PerformanceDataWriter(File("test-results/performance-serialization")).write(
                    "test2_constant_time_growth",
                    meta(
                        "Test 2 - Cache Lookup Stability",
                        "Verify cache hit time is stable across repeated calls (P90/P10 spread, fixed batch size)",
                        "cache lookup is O(1) with JVM warmup()",
                        warmupIterations,
                        listOf(numSamples),
                        numSamples,
                        true,
                        200,
                        "P90/P10 < 20x, median < 10µs",
                    ),
                    dataPoints,
                )
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun dp(
        testName: String,
        testId: String,
        executionId: String,
        date: String,
        time: String,
        sampleSize: Int,
        runNumber: Int,
        timeNs: Long,
        warmupIterations: Int,
        si: SystemInfo,
        testResult: String,
        testStatus: String,
        failureReason: String,
        metrics: Map<String, String>,
    ) = PerformanceDataPoint(
        testName = testName,
        testId = testId,
        executionId = executionId,
        executionDate = date,
        executionTime = time,
        sampleSize = sampleSize,
        runNumber = runNumber,
        timeNs = timeNs,
        warmupIterations = warmupIterations,
        jvmVersion = si.jvmVersion,
        jvmVendor = si.jvmVendor,
        osName = si.osName,
        osVersion = si.osVersion,
        osArch = si.osArch,
        cpuCores = si.cpuCores,
        maxMemoryMb = si.maxMemoryMb,
        totalMemoryMb = si.totalMemoryMb,
        freeMemoryMb = si.freeMemoryMb,
        kotlinVersion = si.kotlinVersion,
        gradleVersion = si.gradleVersion,
        testResult = testResult,
        testStatus = testStatus,
        failureReason = failureReason,
        metrics = metrics,
    )

    private fun meta(
        testName: String,
        description: String,
        testMethod: String,
        warmupIterations: Int,
        sampleSizes: List<Int>,
        runsPerSample: Int,
        gcBeforeTest: Boolean,
        postGcSleepMs: Long,
        passThreshold: String,
    ) = TestMetadata(
        testName = testName,
        description = description,
        testClass = "spark.kotlin.serialization.SerializationCachePerformanceTest",
        testMethod = testMethod,
        warmupIterations = warmupIterations,
        sampleSizes = sampleSizes,
        runsPerSample = runsPerSample,
        blackholeEnabled = true,
        gcBeforeTest = gcBeforeTest,
        postGcSleepMs = postGcSleepMs,
        passThreshold = passThreshold,
    )
}
