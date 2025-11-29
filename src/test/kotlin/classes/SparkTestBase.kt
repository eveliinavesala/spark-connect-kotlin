package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.io.File

@Testcontainers
abstract class SparkTestBase {

    companion object {
        private val projectRoot = System.getProperty("user.dir")

        @Container
        @JvmField
        @Suppress("DEPRECATION") // withFileSystemBind is deprecated but reliable
        val sparkContainer: GenericContainer<*> = GenericContainer("spark-server")
            .withExposedPorts(15002)
            // Mount the data directory to an absolute path inside the container
            .withFileSystemBind("$projectRoot/src/test/resources/data", "/data")

        lateinit var spark: SparkSession

        @BeforeAll
        @JvmStatic
        fun setup() {
            spark = SparkSession.builder()
                .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
                .getOrCreate()

            // Find the fat JAR file and add it to the Spark session.
            val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-fat.jar")
            
            if (jarFile.exists()) {
                spark.addArtifact(jarFile.toURI())
            } else {
                // Fallback for running in the IDE
                val mainClasses = File(projectRoot, "build/classes/kotlin/main")
                val testClasses = File(projectRoot, "build/classes/kotlin/test")
                if (mainClasses.exists()) spark.addArtifact(mainClasses.toURI())
                if (testClasses.exists()) spark.addArtifact(testClasses.toURI())
            }
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            if (::spark.isInitialized) {
                spark.stop()
            }
        }
    }
}
