package classes

import org.apache.spark.sql.SparkSession
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration

/**
 * A true singleton object to manage a single, shared Spark container and SparkSession
 * for the entire test suite (both Kotlin and Java).
 */
object SparkContainerManager {
    private val projectRoot = System.getProperty("user.dir")

    val sparkContainer: GenericContainer<*> by lazy {
        println("--- Starting single Spark container for the test suite ---")
        // Use the image name defined in the Makefile
        GenericContainer("spark-server:latest")
            .withExposedPorts(15002)
            .withCopyToContainer(
                MountableFile.forClasspathResource("data"),
                "/data",
            ).withCommand(
                "bash",
                "-c",
                "/opt/spark/bin/spark-submit " +
                    "--class org.apache.spark.sql.connect.service.SparkConnectServer " +
                    "--master local[*] " +
                    "--conf spark.connect.grpc.binding.port=15002 " +
                    "/opt/spark/jars/spark-connect_*.jar",
            ).waitingFor(
                Wait
                    .forLogMessage(".*Spark Connect server started.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)),
            ).apply { start() }
    }

    // Singleton SparkSession
    val sparkSession: SparkSession by lazy {
        val session =
            SparkSession
                .builder()
                .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
                .getOrCreate()

        // Add the test fat jar as an artifact
        val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-test-fat.jar")

        if (jarFile.exists()) {
            session.addArtifact(jarFile.toURI())
        } else {
            error("Test fat JAR not found at ${jarFile.absolutePath}. Please run the 'testFatJar' task.")
        }
        session
    }
}
