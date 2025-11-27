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
        @Container
        @JvmField
        val sparkContainer: GenericContainer<*> = GenericContainer("spark-server")
            .withExposedPorts(15002)

        lateinit var spark: SparkSession

        @BeforeAll
        @JvmStatic
        fun setup() {
            spark = SparkSession.builder()
                .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
                .getOrCreate()

            // Find the fat JAR file created by the 'jar' task in build.gradle.kts
            // and add it to the Spark session. This is crucial for UDFs.
            val projectDir = System.getProperty("user.dir")
            val jarFile = File(projectDir, "build/libs/spark-connect-kotlin-1.0-fat.jar")
            
            if (jarFile.exists()) {
                spark.addArtifact(jarFile.toURI())
            } else {
                // This is a fallback for running tests directly in the IDE, where the JAR
                // might not be built automatically. It adds the compiled class directories.
                val mainClasses = File(projectDir, "build/classes/kotlin/main")
                val testClasses = File(projectDir, "build/classes/kotlin/test")
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
