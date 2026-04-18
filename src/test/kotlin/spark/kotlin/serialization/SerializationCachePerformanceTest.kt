package spark.kotlin.serialization

import kotlinx.serialization.Serializable
import kotlinx.serialization.serializer
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.io.BufferedWriter
import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.math.abs
import kotlin.math.pow
import kotlin.system.measureNanoTime
import kotlin.test.assertTrue

/**
 * Performance tests for [SerializationCache] verifying O(1) complexity of cached lookups.
 *
 * Mirrors [spark.kotlin.reflect.ReflectionCacheTest] for structural parity.
 * [SerializationCache] is keyed by [kotlinx.serialization.descriptors.SerialDescriptor]
 * (accessed via [kotlinx.serialization.KSerializer]) rather than by [kotlin.reflect.KType].
 *
 * Five tests are applied:
 * 1. Measurement validation — confirms the benchmarking method distinguishes O(1) from O(n).
 * 2. Lookup stability — P90/P10 spread across 50 samples with a fixed batch size.
 * 3. Linear regression — slope near zero proves time does not grow with call count.
 * 4. Memory pressure — lookup remains within 10× of baseline under 100 MB allocation.
 * 5. Descriptor complexity independence — lookup time does not depend on field count.
 *
 * Output CSVs are written to `test-results/performance-serialization/` to avoid
 * collisions with ReflectionCacheTest output in `test-results/performance/`.
 */
class SerializationCachePerformanceTest {

    // ── Serializable test types ───────────────────────────────────────────────
    // Must be class-level: the Kotlin compiler cannot generate serializers for local classes.

    @Serializable
    data class ComplexType(
        val id: Int,
        val name: String,
        val description: String,
        val value: Double,
        val active: Boolean,
        val tags: List<String>,
        val metadata: Map<String, Int>
    )

    @Serializable data class Tiny(val x: Int)
    @Serializable data class Small(val a: String, val b: Int)
    @Serializable data class Large(
        val f1: String, val f2: Int,  val f3: Double,  val f4: Boolean,
        val f5: String, val f6: Int,  val f7: Double,  val f8: Boolean,
        val f9: List<String>, val f10: Map<String, Int>
    )

    @Suppress("UNUSED_PARAMETER")
    private fun blackhole(value: Any?) { /* prevents dead-code elimination */ }

    // ── Test 1: Measurement validation ───────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("Measurement validation: Compare known O(1) vs O(n)")
    fun `measurement method correctly distinguishes O(1) from O(n)`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints  = mutableListOf<PerformanceDataPoint>()
        var testResult  = "PASSED"
        var testStatus  = "completed"
        var failureReason = ""
        val warmupIterations = 10_000

        try {
            println("\n=== Test 1: Measurement Validation ===")
            println("Purpose: Verify our benchmarking methodology works correctly")
            println("Expected: HashMap stays constant, ArrayList grows linearly\n")

            val sampleSizes = listOf(10_000, 50_000, 100_000)

            println("HashMap (O(1)) lookups:")
            val hashMapTimes = sampleSizes.map { size ->
                val si = SystemInfoCollector.collect()
                val (date, time) = getCurrentTimestamp()
                val map = (1..size).associateBy { it }
                val target = size / 2
                var result: Int? = null
                val timeNs = measureNanoTime { repeat(10_000) { result = map[target] } } / 10_000
                blackhole(result)
                println("  n=$size: ${timeNs}ns")
                dataPoints += dp("test1_measurement_validation", "test1", executionId, date, time,
                    size, 1, timeNs, 0, si, testResult, testStatus, failureReason,
                    mapOf("test1_data_structure" to "HashMap", "test1_expected_complexity" to "O(1)"))
                size to timeNs
            }

            println("\nArrayList.contains (O(n)) lookups:")
            val arrayListTimes = sampleSizes.map { size ->
                val si = SystemInfoCollector.collect()
                val (date, time) = getCurrentTimestamp()
                val list = (1..size).toList()
                var result = false
                val timeNs = measureNanoTime { repeat(1_000) { result = list.contains(size) } } / 1_000
                blackhole(result)
                println("  n=$size: ${timeNs}ns")
                dataPoints += dp("test1_measurement_validation", "test1", executionId, date, time,
                    size, 1, timeNs, 0, si, testResult, testStatus, failureReason,
                    mapOf("test1_data_structure" to "ArrayList", "test1_expected_complexity" to "O(n)"))
                size to timeNs
            }

            val hashMapGrowth   = hashMapTimes.last().second.toDouble()   / hashMapTimes.first().second
            val arrayListGrowth = arrayListTimes.last().second.toDouble() / arrayListTimes.first().second
            println("\nHashMap growth (10k→100k): ${String.format("%.2f", hashMapGrowth)}x")
            println("ArrayList growth (10k→100k): ${String.format("%.2f", arrayListGrowth)}x")

            assertTrue(hashMapGrowth < 3.0,
                "HashMap should be O(1), growth was ${String.format("%.2f", hashMapGrowth)}x")
            assertTrue(arrayListGrowth > 2.0,
                "ArrayList.contains should be O(n), growth was ${String.format("%.2f", arrayListGrowth)}x")
            println("✓ Measurement method valid: correctly identifies O(1) vs O(n)")

            println("\nSerializationCache.getSchema:")
            val ser = serializer<ComplexType>()
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }
            val si = SystemInfoCollector.collect()
            val (date, time) = getCurrentTimestamp()
            var cacheResult: StructType? = null
            val cacheTime = measureNanoTime {
                repeat(10_000) { cacheResult = SerializationCache.getSchema(ser) }
            } / 10_000
            blackhole(cacheResult)
            println("  Cache lookup: ${cacheTime}ns")
            dataPoints += dp("test1_measurement_validation", "test1", executionId, date, time,
                10_000, 1, cacheTime, warmupIterations, si, testResult, testStatus, failureReason,
                mapOf("test1_data_structure" to "SerializationCache",
                      "test1_expected_complexity" to "O(1)",
                      "test1_structure_growth_10k_100k" to "N/A"))
            val cacheVsHashMap = cacheTime.toDouble() / hashMapTimes.first().second
            assertTrue(cacheVsHashMap < 100.0,
                "Cache should have O(1) characteristics, was ${String.format("%.1f", cacheVsHashMap)}x slower than HashMap")
            println("✓ SerializationCache exhibits O(1) characteristics")

        } catch (e: AssertionError) {
            testResult = "FAILED"; testStatus = "failed_assertion"; failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"; testStatus = "failed_error"; failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) PerformanceDataWriter().write("test1_measurement_validation",
                meta("Test 1 - Measurement Validation",
                    "Verify benchmarking methodology correctly distinguishes O(1) from O(n)",
                    "measurement method correctly distinguishes O(1) from O(n)()",
                    warmupIterations, listOf(10_000, 50_000, 100_000), 1,
                    false, 0, "HashMap growth < 3.0x, ArrayList growth > 2.0x"), dataPoints)
        }
    }

    // ── Test 2: Lookup stability ──────────────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) complexity with proper JVM warmup")
    fun `cache lookup is O(1) with JVM warmup`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints  = mutableListOf<PerformanceDataPoint>()
        var testResult  = "PASSED"
        var testStatus  = "completed"
        var failureReason = ""
        val warmupIterations = 300_000
        val BATCH_SIZE   = 10_000
        val NUM_SAMPLES  = 50

        try {
            println("\n=== Test 2: Cache Lookup Stability (Percentile-based) ===")
            println("Purpose: Verify cache hit time is stable — not growing with call count")
            println("Method:  $NUM_SAMPLES samples × $BATCH_SIZE ops, fixed batch size eliminates JIT loop variance")
            println("Expected: P90/P10 spread < 20x, median < 10µs\n")

            val ser = serializer<ComplexType>()
            println("Warming up JVM ($warmupIterations iterations)...")
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }
            System.gc(); Thread.sleep(200)

            println("Collecting $NUM_SAMPLES samples...")
            val samplesNs = LongArray(NUM_SAMPLES) { sampleIdx ->
                val si = SystemInfoCollector.collect()
                val (date, time) = getCurrentTimestamp()
                var result: StructType? = null
                val total = measureNanoTime { repeat(BATCH_SIZE) { result = SerializationCache.getSchema(ser) } }
                blackhole(result)
                val perOpNs = total / BATCH_SIZE
                dataPoints += dp("test2_constant_time_growth", "test2", executionId, date, time,
                    BATCH_SIZE, sampleIdx + 1, perOpNs, warmupIterations, si,
                    testResult, testStatus, failureReason, emptyMap())
                perOpNs
            }

            samplesNs.sort()
            val p10    = samplesNs[NUM_SAMPLES / 10]
            val p50    = samplesNs[NUM_SAMPLES / 2]
            val p90    = samplesNs[NUM_SAMPLES * 9 / 10]
            val p99    = samplesNs[(NUM_SAMPLES * 99 / 100).coerceAtMost(NUM_SAMPLES - 1)]
            val spread = p90.toDouble() / p10.coerceAtLeast(1L)
            println("p10=${p10}ns  p50=${p50}ns  p90=${p90}ns  p99=${p99}ns")
            println("P90/P10 spread: ${String.format("%.1f", spread)}x")

            val metrics = mapOf(
                "test2_p10_ns"           to p10.toString(),
                "test2_p50_ns"           to p50.toString(),
                "test2_p90_ns"           to p90.toString(),
                "test2_p99_ns"           to p99.toString(),
                "test2_spread_p90_p10"   to String.format("%.2f", spread),
                "test2_spread_threshold" to "20.0",
                "test2_p50_threshold_ns" to "10000"
            )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            assertTrue(spread < 20.0,
                "P90/P10 spread ${String.format("%.1f", spread)}x exceeds 20x — lookup is not stable")
            assertTrue(p50 < 10_000L,
                "Median lookup ${p50}ns exceeds 10µs — cache may be recomputing on every call")
            println("✓ O(1) confirmed: spread=${String.format("%.1f", spread)}x, median=${p50}ns")

        } catch (e: AssertionError) {
            testResult = "FAILED"; testStatus = "failed_assertion"; failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"; testStatus = "failed_error"; failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) PerformanceDataWriter().write("test2_constant_time_growth",
                meta("Test 2 - Cache Lookup Stability",
                    "Verify cache hit time is stable across repeated calls (P90/P10 spread, fixed batch size)",
                    "cache lookup is O(1) with JVM warmup()",
                    warmupIterations, listOf(NUM_SAMPLES), NUM_SAMPLES, true, 200, "P90/P10 < 20x, median < 10µs"), dataPoints)
        }
    }

    // ── Test 3: Linear regression ─────────────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("Linear regression: slope near zero proves O(1)")
    fun `linear regression confirms constant time complexity`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints  = mutableListOf<PerformanceDataPoint>()
        var testResult  = "PASSED"
        var testStatus  = "completed"
        var failureReason = ""
        val warmupIterations = 100_000

        try {
            println("\n=== Test 3: Linear Regression Analysis ===")
            println("Purpose: Verify time doesn't grow with iteration count")
            println("Expected: Slope ≈ 0 (time independent of n)\n")

            val ser = serializer<ComplexType>()
            println("Warming up JVM (100k iterations)...")
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }
            System.gc(); Thread.sleep(200)

            println("Collecting measurements...")
            val regressionPoints = mutableListOf<Pair<Int, Long>>()
            val sampleSizes = (1..20).map { it * 10_000 }

            for (n in sampleSizes) {
                val timings = (1..5).map { runNum ->
                    val si = SystemInfoCollector.collect()
                    val (date, time) = getCurrentTimestamp()
                    var result: StructType? = null
                    val timeNs = measureNanoTime { repeat(n) { result = SerializationCache.getSchema(ser) } } / n
                    blackhole(result)
                    dataPoints += dp("test3_linear_regression", "test3", executionId, date, time,
                        n, runNum, timeNs, warmupIterations, si, testResult, testStatus, failureReason, emptyMap())
                    timeNs
                }
                regressionPoints.add(n to timings.average().toLong())
            }

            val (slope, intercept, rSquared) = linearRegression(regressionPoints)
            val slopeThreshold = 0.1
            println("\nLinear Regression Results:")
            println("  Time = ${String.format("%.6f", slope)} * n + ${String.format("%.2f", intercept)}")
            println("  R² = ${String.format("%.4f", rSquared)}")
            println("  Slope = ${String.format("%.6f", slope)} ns/operation")

            val metrics = mapOf(
                "test3_regression_slope"     to slope.toString(),
                "test3_regression_intercept" to intercept.toString(),
                "test3_r_squared"            to rSquared.toString(),
                "test3_slope_threshold"      to slopeThreshold.toString()
            )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            assertTrue(abs(slope) < slopeThreshold,
                "Slope should be near 0 for O(1), was ${String.format("%.6f", slope)}")
            println("✓ Slope near zero confirms O(1) complexity")

        } catch (e: AssertionError) {
            testResult = "FAILED"; testStatus = "failed_assertion"; failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"; testStatus = "failed_error"; failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) PerformanceDataWriter().write("test3_linear_regression",
                meta("Test 3 - Linear Regression Analysis",
                    "Verify time doesn't grow with iteration count using linear regression",
                    "linear regression confirms constant time complexity()",
                    warmupIterations, (1..20).map { it * 10_000 }, 5, true, 200, "abs(slope) < 0.1"), dataPoints)
        }
    }

    // ── Test 4: Memory pressure ───────────────────────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) performance maintained under memory pressure")
    fun `cache performance stable under memory pressure`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints  = mutableListOf<PerformanceDataPoint>()
        var testResult  = "PASSED"
        var testStatus  = "completed"
        var failureReason = ""
        val warmupIterations  = 100_000
        val memoryPressureMb  = 100

        try {
            println("\n=== Test 4: Memory Pressure Stability ===")
            println("Purpose: Verify O(1) performance maintained under GC pressure")
            println("Expected: Performance degradation < 10x\n")

            val ser = serializer<ComplexType>()
            println("Measuring baseline performance...")
            repeat(warmupIterations) { blackhole(SerializationCache.getSchema(ser)) }

            val siBase = SystemInfoCollector.collect()
            val (dateBase, timeBase) = getCurrentTimestamp()
            var result: StructType? = null
            val baselineTime = measureNanoTime { repeat(100_000) { result = SerializationCache.getSchema(ser) } } / 100_000
            blackhole(result)
            println("Baseline: ${baselineTime}ns")
            dataPoints += dp("test4_memory_pressure", "test4", executionId, dateBase, timeBase,
                100_000, 1, baselineTime, warmupIterations, siBase, testResult, testStatus, failureReason, emptyMap())

            println("Creating memory pressure (${memoryPressureMb}MB)...")
            val pressure = (1..memoryPressureMb).map { ByteArray(1024 * 1024) }

            val siPress = SystemInfoCollector.collect()
            val (datePress, timePress) = getCurrentTimestamp()
            result = null
            val pressureTime = measureNanoTime { repeat(100_000) { result = SerializationCache.getSchema(ser) } } / 100_000
            blackhole(result)
            blackhole(pressure) // keep allocation alive
            println("Under pressure: ${pressureTime}ns")
            dataPoints += dp("test4_memory_pressure", "test4", executionId, datePress, timePress,
                100_000, 2, pressureTime, warmupIterations, siPress, testResult, testStatus, failureReason, emptyMap())

            val degradation = pressureTime.toDouble() / baselineTime
            val degradationThreshold = 10.0
            println("Degradation: ${String.format("%.2f", degradation)}x")

            val metrics = mapOf(
                "test4_baseline_time_ns"      to baselineTime.toString(),
                "test4_pressure_time_ns"      to pressureTime.toString(),
                "test4_degradation_ratio"     to degradation.toString(),
                "test4_degradation_threshold" to degradationThreshold.toString(),
                "test4_memory_pressure_mb"    to memoryPressureMb.toString()
            )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            assertTrue(degradation >= 0.2 && degradation < degradationThreshold,
                "Performance should stay within ${degradationThreshold}x under memory pressure, was ${String.format("%.2f", degradation)}x")
            println("✓ O(1) maintained under memory pressure")

        } catch (e: AssertionError) {
            testResult = "FAILED"; testStatus = "failed_assertion"; failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"; testStatus = "failed_error"; failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) PerformanceDataWriter().write("test4_memory_pressure",
                meta("Test 4 - Memory Pressure Stability",
                    "Verify O(1) performance maintained under GC pressure",
                    "cache performance stable under memory pressure()",
                    warmupIterations, listOf(100_000), 2, false, 0, "0.2 <= degradation < 10.0"), dataPoints)
        }
    }

    // ── Test 5: Descriptor complexity independence ────────────────────────────

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) holds across different descriptor complexities")
    fun `cache performance independent of descriptor complexity`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints  = mutableListOf<PerformanceDataPoint>()
        var testResult  = "PASSED"
        var testStatus  = "completed"
        var failureReason = ""
        val warmupIterations = 20_000
        val BATCH_SIZE       = 10_000
        val SAMPLES_PER_TYPE = 20

        try {
            println("\n=== Test 5: Descriptor Complexity Independence ===")
            println("Purpose: Verify lookup time doesn't depend on descriptor field count")
            println("Expected: Similar times for small and large descriptors\n")

            val types = listOf(
                Triple("Tiny",  1,  serializer<Tiny>()),
                Triple("Small", 2,  serializer<Small>()),
                Triple("Large", 10, serializer<Large>())
            )

            println("Warming up all caches (interleaved)...")
            repeat(warmupIterations) { i -> blackhole(SerializationCache.getSchema(types[i % types.size].third)) }
            System.gc(); Thread.sleep(200)

            println("\nCache lookup times by descriptor complexity ($SAMPLES_PER_TYPE samples each, median):")
            val medians = mutableListOf<Long>()

            types.forEachIndexed { typeIndex, (complexity, fieldCount, ser) ->
                val perOpSamples = LongArray(SAMPLES_PER_TYPE) { sampleIdx ->
                    val si = SystemInfoCollector.collect()
                    val (date, time) = getCurrentTimestamp()
                    var result: StructType? = null
                    val total = measureNanoTime { repeat(BATCH_SIZE) { result = SerializationCache.getSchema(ser) } }
                    blackhole(result)
                    val perOpNs = total / BATCH_SIZE
                    dataPoints += dp("test5_type_complexity", "test5", executionId, date, time,
                        BATCH_SIZE, typeIndex * SAMPLES_PER_TYPE + sampleIdx + 1,
                        perOpNs, warmupIterations, si, testResult, testStatus, failureReason,
                        mapOf("test5_type_complexity" to complexity, "test5_field_count" to fieldCount.toString()))
                    perOpNs
                }
                perOpSamples.sort()
                val median = perOpSamples[SAMPLES_PER_TYPE / 2]
                medians.add(median)
                println("  $complexity ($fieldCount fields): median=${median}ns  p90=${perOpSamples[SAMPLES_PER_TYPE * 9 / 10]}ns")
            }

            val variance = medians.maxOrNull()!!.toDouble() / medians.minOrNull()!!.coerceAtLeast(1L)
            val varianceThreshold = 15.0
            println("\nMedian spread (max/min): ${String.format("%.2f", variance)}x")

            dataPoints.replaceAll {
                it.copy(metrics = it.metrics + mapOf(
                    "test5_variance_ratio"     to String.format("%.2f", variance),
                    "test5_variance_threshold" to varianceThreshold.toString()
                ))
            }

            assertTrue(variance < varianceThreshold,
                "Median spread across descriptors ${String.format("%.2f", variance)}x exceeds ${varianceThreshold}x")
            println("✓ O(1) independent of descriptor complexity (spread=${String.format("%.1f", variance)}x)")

        } catch (e: AssertionError) {
            testResult = "FAILED"; testStatus = "failed_assertion"; failureReason = e.message ?: "Assertion failed"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } catch (e: Exception) {
            testResult = "FAILED"; testStatus = "failed_error"; failureReason = "${e.javaClass.simpleName}: ${e.message}"
            dataPoints.replaceAll { it.copy(testResult = testResult, testStatus = testStatus, failureReason = failureReason) }
            throw e
        } finally {
            if (dataPoints.isNotEmpty()) PerformanceDataWriter().write("test5_type_complexity",
                meta("Test 5 - Descriptor Complexity Independence",
                    "Verify lookup time doesn't depend on descriptor field count (median of 20 samples, interleaved warmup)",
                    "cache performance independent of descriptor complexity()",
                    warmupIterations, listOf(BATCH_SIZE), SAMPLES_PER_TYPE, true, 200, "median spread < 15x"), dataPoints)
        }
    }

    // ── Infrastructure ────────────────────────────────────────────────────────

    data class PerformanceDataPoint(
        val testName: String, val testId: String, val executionId: String,
        val executionDate: String, val executionTime: String,
        val sampleSize: Int, val runNumber: Int, val timeNs: Long, val warmupIterations: Int,
        val jvmVersion: String, val jvmVendor: String,
        val osName: String, val osVersion: String, val osArch: String,
        val cpuCores: Int, val maxMemoryMb: Long, val totalMemoryMb: Long, val freeMemoryMb: Long,
        val kotlinVersion: String, val gradleVersion: String,
        val testResult: String, val testStatus: String, val failureReason: String = "",
        val metrics: Map<String, String> = emptyMap()
    )

    data class TestMetadata(
        val testName: String, val description: String, val testClass: String, val testMethod: String,
        val warmupIterations: Int, val sampleSizes: List<Int>, val runsPerSample: Int,
        val blackholeEnabled: Boolean, val gcBeforeTest: Boolean, val postGcSleepMs: Long,
        val passThreshold: String, val additionalConfig: Map<String, String> = emptyMap()
    )

    data class SystemInfo(
        val jvmVersion: String, val jvmVendor: String,
        val osName: String, val osVersion: String, val osArch: String,
        val cpuCores: Int, val maxMemoryMb: Long, val totalMemoryMb: Long, val freeMemoryMb: Long,
        val kotlinVersion: String, val gradleVersion: String
    )

    object SystemInfoCollector {
        fun collect(): SystemInfo {
            val rt = Runtime.getRuntime()
            return SystemInfo(
                jvmVersion    = System.getProperty("java.version"),
                jvmVendor     = System.getProperty("java.vendor"),
                osName        = System.getProperty("os.name"),
                osVersion     = System.getProperty("os.version"),
                osArch        = System.getProperty("os.arch"),
                cpuCores      = rt.availableProcessors(),
                maxMemoryMb   = rt.maxMemory()   / (1024 * 1024),
                totalMemoryMb = rt.totalMemory() / (1024 * 1024),
                freeMemoryMb  = rt.freeMemory()  / (1024 * 1024),
                kotlinVersion = KotlinVersion.CURRENT.toString(),
                gradleVersion = runCatching {
                    File("gradle/wrapper/gradle-wrapper.properties").readLines()
                        .find { it.contains("distributionUrl") }
                        ?.let { Regex("gradle-([\\d.]+)").find(it)?.groupValues?.get(1) } ?: "unknown"
                }.getOrDefault("unknown")
            )
        }
    }

    object ExecutionIdGenerator {
        fun generate(): String = "exec_${UUID.randomUUID().toString().replace("-", "").substring(0, 12)}"
    }

    private fun getCurrentTimestamp(): Pair<String, String> {
        val now = Instant.now().atZone(ZoneOffset.UTC)
        return Pair(
            now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
            now.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))
        )
    }

    private fun linearRegression(points: List<Pair<Int, Long>>): Triple<Double, Double, Double> {
        val n     = points.size.toDouble()
        val sumX  = points.sumOf { it.first.toDouble() }
        val sumY  = points.sumOf { it.second.toDouble() }
        val sumXY = points.sumOf { it.first.toDouble() * it.second }
        val sumX2 = points.sumOf { it.first.toDouble().pow(2) }
        val slope     = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX.pow(2))
        val intercept = (sumY - slope * sumX) / n
        val meanY     = sumY / n
        val ssTotal   = points.sumOf { (it.second - meanY).pow(2) }
        val ssResid   = points.sumOf { val p = slope * it.first + intercept; (it.second - p).pow(2) }
        return Triple(slope, intercept, 1 - ssResid / ssTotal)
    }

    // ── Helpers to reduce repetition ─────────────────────────────────────────

    private fun dp(
        testName: String, testId: String, executionId: String,
        date: String, time: String, sampleSize: Int, runNumber: Int,
        timeNs: Long, warmupIterations: Int, si: SystemInfo,
        testResult: String, testStatus: String, failureReason: String,
        metrics: Map<String, String>
    ) = PerformanceDataPoint(
        testName = testName, testId = testId, executionId = executionId,
        executionDate = date, executionTime = time,
        sampleSize = sampleSize, runNumber = runNumber, timeNs = timeNs,
        warmupIterations = warmupIterations,
        jvmVersion = si.jvmVersion, jvmVendor = si.jvmVendor,
        osName = si.osName, osVersion = si.osVersion, osArch = si.osArch,
        cpuCores = si.cpuCores, maxMemoryMb = si.maxMemoryMb,
        totalMemoryMb = si.totalMemoryMb, freeMemoryMb = si.freeMemoryMb,
        kotlinVersion = si.kotlinVersion, gradleVersion = si.gradleVersion,
        testResult = testResult, testStatus = testStatus, failureReason = failureReason,
        metrics = metrics
    )

    private fun meta(
        testName: String, description: String, testMethod: String,
        warmupIterations: Int, sampleSizes: List<Int>, runsPerSample: Int,
        gcBeforeTest: Boolean, postGcSleepMs: Long, passThreshold: String
    ) = TestMetadata(
        testName = testName, description = description,
        testClass = "spark.kotlin.serialization.SerializationCachePerformanceTest",
        testMethod = testMethod,
        warmupIterations = warmupIterations, sampleSizes = sampleSizes,
        runsPerSample = runsPerSample, blackholeEnabled = true,
        gcBeforeTest = gcBeforeTest, postGcSleepMs = postGcSleepMs,
        passThreshold = passThreshold
    )

    class PerformanceDataWriter(private val outputDir: File = File("test-results/performance-serialization")) {
        init { outputDir.mkdirs() }

        fun write(testId: String, metadata: TestMetadata, dataPoints: List<PerformanceDataPoint>) {
            if (dataPoints.isEmpty()) return
            val file  = File(outputDir, "$testId.csv")
            file.bufferedWriter().use { w ->
                w.write("# Test: ${metadata.testName}\n")
                w.write("# Description: ${metadata.description}\n")
                w.write("# Test Class: ${metadata.testClass}\n")
                w.write("# Test Method: ${metadata.testMethod}\n")
                w.write("# Warmup Iterations: ${metadata.warmupIterations}\n")
                w.write("# Sample Sizes: ${metadata.sampleSizes.joinToString(", ")}\n")
                w.write("# Runs Per Sample: ${metadata.runsPerSample}\n")
                w.write("# Blackhole: ${if (metadata.blackholeEnabled) "Enabled" else "Disabled"}\n")
                w.write("# GC Before Test: ${metadata.gcBeforeTest}\n")
                w.write("# Post-GC Sleep: ${metadata.postGcSleepMs}ms\n")
                w.write("# Pass Threshold: ${metadata.passThreshold}\n#\n")
                val baseHeaders = listOf(
                    "test_name","test_id","execution_id","execution_date","execution_time",
                    "sample_size","run_number","time_ns","warmup_iterations",
                    "jvm_version","jvm_vendor","os_name","os_version","os_arch",
                    "cpu_cores","max_memory_mb","total_memory_mb","free_memory_mb",
                    "kotlin_version","gradle_version","test_result","test_status","failure_reason"
                )
                val metricHeaders = dataPoints.first().metrics.keys.sorted()
                w.write((baseHeaders + metricHeaders).joinToString(",") + "\n")
                val allMetricKeys = dataPoints.flatMap { it.metrics.keys }.toSet().sorted()
                dataPoints.forEach { p ->
                    val vals = listOf(
                        p.testName, p.testId, p.executionId, p.executionDate, p.executionTime,
                        p.sampleSize.toString(), p.runNumber.toString(), p.timeNs.toString(),
                        p.warmupIterations.toString(),
                        p.jvmVersion, p.jvmVendor, p.osName, p.osVersion, p.osArch,
                        p.cpuCores.toString(), p.maxMemoryMb.toString(),
                        p.totalMemoryMb.toString(), p.freeMemoryMb.toString(),
                        p.kotlinVersion, p.gradleVersion,
                        p.testResult, p.testStatus, p.failureReason
                    ) + allMetricKeys.map { p.metrics[it] ?: "" }
                    w.write(vals.joinToString(",") { v ->
                        if (',' in v || '"' in v) "\"${v.replace("\"", "\"\"")}\"" else v
                    } + "\n")
                }
            }
            println("✓ Performance data written to: ${file.absolutePath}")
            println("  - Execution ID: ${dataPoints.firstOrNull()?.executionId}")
            println("  - Data points: ${dataPoints.size}")
            println("  - Test result: ${dataPoints.firstOrNull()?.testResult}")
        }
    }
}
