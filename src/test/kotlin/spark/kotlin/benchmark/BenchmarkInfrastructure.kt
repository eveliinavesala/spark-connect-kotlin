package spark.kotlin.benchmark

import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import kotlin.math.pow

/**
 * Shared data types and utilities for benchmark tests in [ReflectionCacheTest],
 * [ReflectionCacheStabilityTest], [SerializationCachePerformanceTest], and
 * [SerializationCacheStabilityTest].
 */

data class PerformanceDataPoint(
    val testName: String,
    val testId: String,
    val executionId: String,
    val executionDate: String,
    val executionTime: String,
    val sampleSize: Int,
    val runNumber: Int,
    val timeNs: Long,
    val warmupIterations: Int,
    val jvmVersion: String,
    val jvmVendor: String,
    val osName: String,
    val osVersion: String,
    val osArch: String,
    val cpuCores: Int,
    val maxMemoryMb: Long,
    val totalMemoryMb: Long,
    val freeMemoryMb: Long,
    val kotlinVersion: String,
    val gradleVersion: String,
    val testResult: String,
    val testStatus: String,
    val failureReason: String = "",
    val metrics: Map<String, String> = emptyMap(),
)

data class TestMetadata(
    val testName: String,
    val description: String,
    val testClass: String,
    val testMethod: String,
    val warmupIterations: Int,
    val sampleSizes: List<Int>,
    val runsPerSample: Int,
    val blackholeEnabled: Boolean,
    val gcBeforeTest: Boolean,
    val postGcSleepMs: Long,
    val passThreshold: String,
    val additionalConfig: Map<String, String> = emptyMap(),
)

data class SystemInfo(
    val jvmVersion: String,
    val jvmVendor: String,
    val osName: String,
    val osVersion: String,
    val osArch: String,
    val cpuCores: Int,
    val maxMemoryMb: Long,
    val totalMemoryMb: Long,
    val freeMemoryMb: Long,
    val kotlinVersion: String,
    val gradleVersion: String,
)

object SystemInfoCollector {
    fun collect(): SystemInfo {
        val runtime = Runtime.getRuntime()
        return SystemInfo(
            jvmVersion = System.getProperty("java.version"),
            jvmVendor = System.getProperty("java.vendor"),
            osName = System.getProperty("os.name"),
            osVersion = System.getProperty("os.version"),
            osArch = System.getProperty("os.arch"),
            cpuCores = runtime.availableProcessors(),
            maxMemoryMb = runtime.maxMemory() / (1024 * 1024),
            totalMemoryMb = runtime.totalMemory() / (1024 * 1024),
            freeMemoryMb = runtime.freeMemory() / (1024 * 1024),
            kotlinVersion = KotlinVersion.CURRENT.toString(),
            gradleVersion = detectGradleVersion(),
        )
    }

    private fun detectGradleVersion(): String =
        try {
            val wrapperProps = File("gradle/wrapper/gradle-wrapper.properties")
            if (wrapperProps.exists()) {
                wrapperProps
                    .readLines()
                    .find { it.contains("distributionUrl") }
                    ?.let { line -> Regex("gradle-([\\d.]+)").find(line)?.groupValues?.get(1) }
                    ?: "unknown"
            } else {
                "unknown"
            }
        } catch (e: Exception) {
            System.err.println("Warning: Failed to detect Gradle version: ${e.message}")
            "unknown"
        }
}

object ExecutionIdGenerator {
    fun generate(): String = "exec_${UUID.randomUUID().toString().replace("-", "").substring(0, 12)}"
}

fun getCurrentTimestamp(): Pair<String, String> {
    val now = Instant.now().atZone(ZoneOffset.UTC)
    return Pair(
        now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
        now.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
    )
}

fun linearRegression(points: List<Pair<Int, Long>>): Triple<Double, Double, Double> {
    val n = points.size.toDouble()
    val sumX = points.sumOf { it.first.toDouble() }
    val sumY = points.sumOf { it.second.toDouble() }
    val sumXY = points.sumOf { it.first.toDouble() * it.second }
    val sumX2 = points.sumOf { it.first.toDouble().pow(2) }
    val slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX.pow(2))
    val intercept = (sumY - slope * sumX) / n
    val meanY = sumY / n
    val ssTotal = points.sumOf { (it.second - meanY).pow(2) }
    val ssResidual =
        points.sumOf {
            val predicted = slope * it.first + intercept
            (it.second - predicted).pow(2)
        }
    return Triple(slope, intercept, 1 - ssResidual / ssTotal)
}

class PerformanceDataWriter(
    private val outputDir: File,
) {
    init {
        outputDir.mkdirs()
    }

    fun write(
        testId: String,
        metadata: TestMetadata,
        dataPoints: List<PerformanceDataPoint>,
    ) {
        if (dataPoints.isEmpty()) {
            println("⚠ No data points to write for $testId")
            return
        }
        val metricKeys = dataPoints.flatMap { it.metrics.keys }.toSet().sorted()
        val headers = buildHeaders(metricKeys)
        val file = File(outputDir, "$testId.csv")

        if (!file.exists()) {
            file.bufferedWriter().use { writer ->
                writeMetadataHeader(writer, metadata)
                writeColumnHeaders(writer, headers)
                writeDataRows(writer, dataPoints, metricKeys)
            }
            println("✓ Performance data written to: ${file.absolutePath}")
        } else {
            val existingHeaders = readExistingHeaders(file)
            if (existingHeaders == headers) {
                BufferedWriter(FileWriter(file, true)).use { writer ->
                    writeDataRows(writer, dataPoints, metricKeys)
                }
                println("✓ Performance data appended to: ${file.absolutePath}")
            } else {
                error(
                    buildString {
                        appendLine("CSV schema mismatch for test '$testId'.")
                        appendLine("Existing file: ${file.absolutePath}")
                        appendLine("To preserve append-only semantics, write is aborted.")
                        appendLine("Delete/reset the file or align metric keys in the test before rerunning.")
                    },
                )
            }
        }

        println("  - Execution ID: ${dataPoints.firstOrNull()?.executionId}")
        println("  - Data points: ${dataPoints.size}")
        println("  - Test result: ${dataPoints.firstOrNull()?.testResult}")
    }

    private fun readExistingHeaders(file: File): List<String>? {
        val headerLine =
            file.useLines { lines ->
                lines.firstOrNull { line ->
                    val trimmed = line.trim()
                    trimmed.isNotEmpty() && !trimmed.startsWith("#")
                }
            }
        return headerLine?.trimEnd('\r')?.split(",")
    }

    private fun writeMetadataHeader(
        writer: BufferedWriter,
        metadata: TestMetadata,
    ) {
        writer.write("# Test: ${metadata.testName}\n")
        writer.write("# Description: ${metadata.description}\n")
        writer.write("# Test Class: ${metadata.testClass}\n")
        writer.write("# Test Method: ${metadata.testMethod}\n")
        writer.write("# Warmup Iterations: ${metadata.warmupIterations}\n")
        writer.write("# Sample Sizes: ${metadata.sampleSizes.joinToString(", ")}\n")
        writer.write("# Runs Per Sample: ${metadata.runsPerSample}\n")
        writer.write("# Blackhole: ${if (metadata.blackholeEnabled) "Enabled" else "Disabled"}\n")
        writer.write("# GC Before Test: ${metadata.gcBeforeTest}\n")
        writer.write("# Post-GC Sleep: ${metadata.postGcSleepMs}ms\n")
        writer.write("# Pass Threshold: ${metadata.passThreshold}\n")
        metadata.additionalConfig.forEach { (key, value) -> writer.write("# $key: $value\n") }
        writer.write("#\n")
    }

    private fun buildHeaders(metricKeys: List<String>): List<String> =
        listOf(
            "test_name",
            "test_id",
            "execution_id",
            "execution_date",
            "execution_time",
            "sample_size",
            "run_number",
            "time_ns",
            "warmup_iterations",
            "jvm_version",
            "jvm_vendor",
            "os_name",
            "os_version",
            "os_arch",
            "cpu_cores",
            "max_memory_mb",
            "total_memory_mb",
            "free_memory_mb",
            "kotlin_version",
            "gradle_version",
            "test_result",
            "test_status",
            "failure_reason",
        ) + metricKeys

    private fun writeColumnHeaders(
        writer: BufferedWriter,
        headers: List<String>,
    ) {
        writer.write(headers.joinToString(","))
        writer.write("\n")
    }

    private fun writeDataRows(
        writer: BufferedWriter,
        dataPoints: List<PerformanceDataPoint>,
        metricKeys: List<String>,
    ) {
        dataPoints.forEach { point ->
            val baseValues =
                listOf(
                    point.testName,
                    point.testId,
                    point.executionId,
                    point.executionDate,
                    point.executionTime,
                    point.sampleSize.toString(),
                    point.runNumber.toString(),
                    point.timeNs.toString(),
                    point.warmupIterations.toString(),
                    point.jvmVersion,
                    point.jvmVendor,
                    point.osName,
                    point.osVersion,
                    point.osArch,
                    point.cpuCores.toString(),
                    point.maxMemoryMb.toString(),
                    point.totalMemoryMb.toString(),
                    point.freeMemoryMb.toString(),
                    point.kotlinVersion,
                    point.gradleVersion,
                    point.testResult,
                    point.testStatus,
                    point.failureReason,
                )
            val metricValues = metricKeys.map { key -> point.metrics[key] ?: "" }
            writer.write((baseValues + metricValues).joinToString(",") { escapeCSV(it) })
            writer.write("\n")
        }
    }

    private fun escapeCSV(value: String): String {
        val needsQuoting =
            value.contains(",") ||
                value.contains("\"") ||
                value.contains("\n") ||
                value.contains("\r")
        return if (needsQuoting) "\"${value.replace("\"", "\"\"")}\"" else value
    }
}
