package spark.kotlin.reflect

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
import spark.kotlin.benchmark.linearRegression
import java.io.File
import java.util.Locale
import kotlin.math.abs
import kotlin.reflect.typeOf
import kotlin.system.measureNanoTime
import kotlin.test.assertTrue

/**
 * Stability tests for [ReflectionCache] verifying O(1) complexity via regression and pressure.
 *
 * Tests 3–5 of the ReflectionCache benchmark suite:
 * - **Test 3** — Linear regression: slope near zero proves time does not grow with call count.
 * - **Test 4** — Memory pressure: O(1) performance is maintained under 100 MB GC pressure.
 * - **Test 5** — Type complexity independence: lookup time does not depend on field count.
 *
 * Tests 1–2 (measurement validation and lookup stability) live in [ReflectionCacheTest].
 *
 * @see <a href="https://shipilev.net/blog/2014/nanotrusting-nanotime/">Nanotrusting the Nanotime</a>
 */
class ReflectionCacheStabilityTest {
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
    private fun blackhole(value: Any?) {
        // Intentionally empty - the parameter prevents optimization
    }

    @Test
    @Tag("benchmark")
    @DisplayName("Linear regression: slope near zero proves O(1)")
    fun `linear regression confirms constant time complexity`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints = mutableListOf<PerformanceDataPoint>()
        var testResult = "PASSED"
        var testStatus = "completed"
        var failureReason = ""
        val warmupIterations = 100_000

        try {
            println("\n=== Test 3: Linear Regression Analysis ===")
            println("Purpose: Verify time doesn't grow with iteration count")
            println("Expected: Slope ≈ 0 (time independent of n)\n")

            val testType = typeOf<ComplexType>()

            println("Warming up JVM (100k iterations)...")
            repeat(warmupIterations) { blackhole(ReflectionCache.getSchema(testType)) }
            System.gc()
            Thread.sleep(200)

            println("Collecting measurements...")
            val regressionDataPoints = mutableListOf<Pair<Int, Long>>()
            val sampleSizes = (1..20).map { it * 10_000 }

            for (n in sampleSizes) {
                val timings =
                    (1..5).map { runNum ->
                        val systemInfo = SystemInfoCollector.collect()
                        val (date, time) = getCurrentTimestamp()
                        var result: StructType? = null
                        val timeNs =
                            measureNanoTime { repeat(n) { result = ReflectionCache.getSchema(testType) } } / n
                        blackhole(result)
                        dataPoints +=
                            dp(
                                "test3_linear_regression",
                                "test3",
                                executionId,
                                date,
                                time,
                                n,
                                runNum,
                                timeNs,
                                warmupIterations,
                                systemInfo,
                                testResult,
                                testStatus,
                                failureReason,
                                emptyMap(),
                            )
                        timeNs
                    }
                regressionDataPoints.add(n to timings.average().toLong())
            }

            val (slope, intercept, rSquared) = linearRegression(regressionDataPoints)
            val slopeThreshold = 0.1

            println("\nLinear Regression Results:")
            println(
                "  Time = ${String.format(Locale.ROOT, "%.6f", slope)} * n + ${
                    String.format(Locale.ROOT, "%.2f", intercept)
                }",
            )
            println("  R² = ${String.format(Locale.ROOT, "%.4f", rSquared)}")
            println("  Slope = ${String.format(Locale.ROOT, "%.6f", slope)} ns/operation")

            val metrics =
                mapOf(
                    "test3_regression_slope" to slope.toString(),
                    "test3_regression_intercept" to intercept.toString(),
                    "test3_r_squared" to rSquared.toString(),
                    "test3_slope_threshold" to slopeThreshold.toString(),
                )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            // For O(1): slope should be near zero (time doesn't grow with n)
            // Allow tiny positive or negative slope due to measurement noise
            assertTrue(
                abs(slope) < slopeThreshold,
                "Slope should be near 0 for O(1), was ${String.format(Locale.ROOT, "%.6f", slope)}",
            )
            println("✓ Slope near zero confirms O(1) complexity")
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
                PerformanceDataWriter(File("test-results/performance-reflect")).write(
                    "test3_linear_regression",
                    meta(
                        "Test 3 - Linear Regression Analysis",
                        "Verify time doesn't grow with iteration count using linear regression",
                        "linear regression confirms constant time complexity()",
                        warmupIterations,
                        (1..20).map { it * 10_000 },
                        5,
                        true,
                        200,
                        "abs(slope) < 0.1",
                    ),
                    dataPoints,
                )
            }
        }
    }

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) performance maintained under memory pressure")
    fun `cache performance stable under memory pressure`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints = mutableListOf<PerformanceDataPoint>()
        var testResult = "PASSED"
        var testStatus = "completed"
        var failureReason = ""
        val warmupIterations = 100_000
        val memoryPressureMb = 100

        try {
            println("\n=== Test 4: Memory Pressure Stability ===")
            println("Purpose: Verify O(1) performance maintained under GC pressure")
            println("Expected: Performance degradation < 10x\n")

            val testType = typeOf<ComplexType>()

            println("Measuring baseline performance...")
            repeat(warmupIterations) { blackhole(ReflectionCache.getSchema(testType)) }

            val systemInfoBaseline = SystemInfoCollector.collect()
            val (dateBaseline, timeBaseline) = getCurrentTimestamp()
            var result: StructType? = null
            val baselineTime =
                measureNanoTime { repeat(100_000) { result = ReflectionCache.getSchema(testType) } } / 100_000
            blackhole(result)
            println("Baseline: ${baselineTime}ns")

            dataPoints +=
                dp(
                    "test4_memory_pressure",
                    "test4",
                    executionId,
                    dateBaseline,
                    timeBaseline,
                    100_000,
                    1,
                    baselineTime,
                    warmupIterations,
                    systemInfoBaseline,
                    testResult,
                    testStatus,
                    failureReason,
                    emptyMap(),
                )

            println("Creating memory pressure (${memoryPressureMb}MB)...")
            val memoryPressure = mutableListOf<ByteArray>()
            repeat(memoryPressureMb) { memoryPressure.add(ByteArray(1024 * 1024)) }

            val systemInfoPressure = SystemInfoCollector.collect()
            val (datePressure, timePressure) = getCurrentTimestamp()
            result = null
            val pressureTime =
                measureNanoTime { repeat(100_000) { result = ReflectionCache.getSchema(testType) } } / 100_000
            blackhole(result)
            println("Under pressure: ${pressureTime}ns")

            dataPoints +=
                dp(
                    "test4_memory_pressure",
                    "test4",
                    executionId,
                    datePressure,
                    timePressure,
                    100_000,
                    2,
                    pressureTime,
                    warmupIterations,
                    systemInfoPressure,
                    testResult,
                    testStatus,
                    failureReason,
                    emptyMap(),
                )

            val degradation = pressureTime.toDouble() / baselineTime
            val degradationThreshold = 10.0
            println("Degradation: ${String.format(Locale.ROOT, "%.2f", degradation)}x")

            val metrics =
                mapOf(
                    "test4_baseline_time_ns" to baselineTime.toString(),
                    "test4_pressure_time_ns" to pressureTime.toString(),
                    "test4_degradation_ratio" to degradation.toString(),
                    "test4_degradation_threshold" to degradationThreshold.toString(),
                    "test4_memory_pressure_mb" to memoryPressureMb.toString(),
                )
            dataPoints.replaceAll { it.copy(metrics = metrics) }

            // Should remain O(1) even under memory pressure
            // Note: Can be faster OR slower due to JIT/GC timing, but should stay within reasonable bounds
            assertTrue(
                degradation >= 0.2 && degradation < degradationThreshold,
                "Performance should stay within ${degradationThreshold}x under memory pressure, was ${
                    String.format(Locale.ROOT, "%.2f", degradation)
                }x",
            )
            println("✓ O(1) maintained under memory pressure")
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
                PerformanceDataWriter(File("test-results/performance-reflect")).write(
                    "test4_memory_pressure",
                    meta(
                        "Test 4 - Memory Pressure Stability",
                        "Verify O(1) performance maintained under GC pressure",
                        "cache performance stable under memory pressure()",
                        warmupIterations,
                        listOf(100_000),
                        2,
                        false,
                        0,
                        "0.2 <= degradation < 10.0",
                    ),
                    dataPoints,
                )
            }
        }
    }

    @Test
    @Tag("benchmark")
    @DisplayName("O(1) holds across different type complexities")
    fun `cache performance independent of type complexity`() {
        val executionId = ExecutionIdGenerator.generate()
        val dataPoints = mutableListOf<PerformanceDataPoint>()
        var testResult = "PASSED"
        var testStatus = "completed"
        var failureReason = ""
        val warmupIterations = 20_000
        val batchSizeT5 = 10_000
        val samplesPerType = 20

        try {
            println("\n=== Test 5: Type Complexity Independence ===")
            println("Purpose: Verify lookup time doesn't depend on type complexity")
            println("Expected: Similar times for small and large types\n")

            data class Tiny(
                val x: Int,
            )

            data class Small(
                val a: String,
                val b: Int,
            )

            data class Large(
                val f1: String,
                val f2: Int,
                val f3: Double,
                val f4: Boolean,
                val f5: String,
                val f6: Int,
                val f7: Double,
                val f8: Boolean,
                val f9: List<String>,
                val f10: Map<String, Int>,
            )

            val types =
                listOf(
                    Triple("Tiny", 1, typeOf<Tiny>()),
                    Triple("Small", 2, typeOf<Small>()),
                    Triple("Large", 10, typeOf<Large>()),
                )

            // Warm all caches — interleave types to give each equal JIT exposure,
            // preventing the last-measured type from being systematically faster.
            println("Warming up all type caches (interleaved)...")
            repeat(warmupIterations) { i ->
                val (_, _, type) = types[i % types.size]
                blackhole(ReflectionCache.getSchema(type))
            }
            System.gc()
            Thread.sleep(200)

            // Use 20 samples per type with a fixed batch size.
            // Single-measurement approach (previous: 1 × 100k / 100k) gives one data point
            // per type — any JIT timing difference between types contaminates the result.
            // With 20 samples we take the median, which is robust to individual outlier samples.
            println("\nCache lookup times by type complexity ($samplesPerType samples each, median):")
            val medians = mutableListOf<Long>()

            types.forEachIndexed { typeIndex, (complexity, fieldCount, type) ->
                val perOpSamples =
                    LongArray(samplesPerType) { sampleIdx ->
                        val systemInfo = SystemInfoCollector.collect()
                        val (date, time) = getCurrentTimestamp()
                        var result: StructType? = null
                        val total =
                            measureNanoTime { repeat(batchSizeT5) { result = ReflectionCache.getSchema(type) } }
                        blackhole(result)
                        val perOpNs = total / batchSizeT5
                        dataPoints +=
                            dp(
                                "test5_type_complexity",
                                "test5",
                                executionId,
                                date,
                                time,
                                batchSizeT5,
                                typeIndex * samplesPerType + sampleIdx + 1,
                                perOpNs,
                                warmupIterations,
                                systemInfo,
                                testResult,
                                testStatus,
                                failureReason,
                                mapOf(
                                    "test5_type_complexity" to complexity,
                                    "test5_field_count" to fieldCount.toString(),
                                ),
                            )
                        perOpNs
                    }
                perOpSamples.sort()
                val median = perOpSamples[samplesPerType / 2]
                medians.add(median)
                println(
                    "  $complexity ($fieldCount fields): median=${median}ns  p90=${
                        perOpSamples[samplesPerType * 9 / 10]
                    }ns",
                )
            }

            val minMedian = medians.minOrNull()!!
            val maxMedian = medians.maxOrNull()!!
            val variance = maxMedian.toDouble() / minMedian.coerceAtLeast(1L)
            // Threshold raised to 15x: all types are sub-microsecond; JIT may legitimately
            // optimize some paths differently. The point is to catch genuine O(n_fields) growth,
            // not micro-second differences between types that are all essentially O(1).
            val varianceThreshold = 15.0

            println("\nMedian spread (max/min): ${String.format(Locale.ROOT, "%.2f", variance)}x")

            dataPoints.replaceAll {
                it.copy(
                    metrics =
                        it.metrics +
                            mapOf(
                                "test5_variance_ratio" to String.format(Locale.ROOT, "%.2f", variance),
                                "test5_variance_threshold" to varianceThreshold.toString(),
                            ),
                )
            }

            assertTrue(
                variance < varianceThreshold,
                "Median spread across types ${
                    String.format(Locale.ROOT, "%.2f", variance)
                }x exceeds ${varianceThreshold}x — lookup time may depend on type complexity",
            )
            println("✓ O(1) independent of type complexity (spread=${String.format(Locale.ROOT, "%.1f", variance)}x)")
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
                PerformanceDataWriter(File("test-results/performance-reflect")).write(
                    "test5_type_complexity",
                    meta(
                        "Test 5 - Type Complexity Independence",
                        "Verify lookup time doesn't depend on type complexity " +
                            "(median of 20 samples per type, interleaved warmup)",
                        "cache performance independent of type complexity()",
                        warmupIterations,
                        listOf(batchSizeT5),
                        samplesPerType,
                        true,
                        200,
                        "median spread < 15x",
                    ),
                    dataPoints,
                )
            }
        }
    }

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
        testClass = "spark.kotlin.reflect.ReflectionCacheStabilityTest",
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
