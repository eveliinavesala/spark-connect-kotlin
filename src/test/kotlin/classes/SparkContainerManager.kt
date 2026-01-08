package classes

import org.apache.spark.sql.SparkSession
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File

/**
 * A true singleton object to manage a single, shared Spark container and SparkSession
 * for the entire test suite (both Kotlin and Java).
 */
object SparkContainerManager {
    private val projectRoot = System.getProperty("user.dir")

    val sparkContainer: GenericContainer<*> by lazy {
        println("--- Starting single Spark container for the test suite ---")
        GenericContainer("spark-server")
            .withExposedPorts(15002)
            .withFileSystemBind("$projectRoot/src/test/resources/data", "/data")
            .waitingFor(Wait.forListeningPort())
            .apply { start() }
    }

    // Singleton SparkSession
    val sparkSession: SparkSession by lazy {
        val session = SparkSession.builder()
            .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
            .getOrCreate()

        // Add the test fat jar as an artifact
        val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-test-fat.jar")
        
        if (jarFile.exists()) {
            session.addArtifact(jarFile.toURI())
        } else {
            throw IllegalStateException("Test fat JAR not found at ${jarFile.absolutePath}. Please run the 'testFatJar' task.")
        }
        session
    }
}
