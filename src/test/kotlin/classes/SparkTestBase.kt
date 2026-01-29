package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.MountableFile
import java.io.File
import java.time.Duration


abstract class SparkTestBase {

    companion object {
        private val projectRoot = System.getProperty("user.dir")

        // Singleton container
        private val sparkContainer: GenericContainer<*> by lazy {
            println("--- Starting single Spark container for the test suite ---")
            GenericContainer("spark:latest")
                .withExposedPorts(15002)
                .withCopyToContainer(
                    MountableFile.forClasspathResource("data"),
                    "/data"
                )
                .withCommand(
                    "bash", "-c",
                    "/opt/spark/bin/spark-submit " +
                    "--class org.apache.spark.sql.connect.service.SparkConnectServer " +
                    "--master local[*] " +
                    "--conf spark.connect.grpc.binding.port=15002 " +
                    "/opt/spark/jars/spark-connect_*.jar"
                )
                .waitingFor(
                    Wait.forLogMessage(".*Spark Connect server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2))
                )
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
            // Testcontainers manages the lifecycle. But good practice and safenet for local runs.
            println("--- Stopping Spark container ---")
            sparkContainer.stop()
        }
    }
}
