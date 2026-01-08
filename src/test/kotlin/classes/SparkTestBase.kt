package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File

abstract class SparkTestBase {

    // Provide direct access to the singleton session for subclasses
    protected val spark: SparkSession
        get() = Companion.spark

    companion object {
        private val projectRoot = System.getProperty("user.dir")

        // Singleton container
        private val sparkContainer: GenericContainer<*> by lazy {
            println("--- Starting single Spark container for the test suite ---")
            GenericContainer("spark-server")
                .withExposedPorts(15002)
                .withFileSystemBind("$projectRoot/src/test/resources/data", "/data")
                .waitingFor(Wait.forListeningPort())
                .apply { start() }
        }

        // Lazy initialization of the SparkSession
        val spark: SparkSession by lazy {
            val session = SparkSession.builder()
                .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
                .getOrCreate()

            val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-test-fat.jar")
            
            if (jarFile.exists()) {
                session.addArtifact(jarFile.toURI())
            } else {
                throw IllegalStateException("Test fat JAR not found at ${jarFile.absolutePath}. Please run the 'testFatJar' task.")
            }
            session
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            // Do not stop the session here. Let Testcontainers manage the lifecycle.
        }
    }
}
