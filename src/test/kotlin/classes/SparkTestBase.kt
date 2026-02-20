package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.time.Duration

abstract class SparkTestBase {

    // Provide direct access to the singleton session for subclasses
    protected val spark: SparkSession
        get() = Companion.spark

    companion object {
        private val projectRoot = System.getProperty("user.dir")

        // Use docker-compose for the full Unity Catalog stack
        private val composeContainer: ComposeContainer by lazy {
            println("--- Starting Unity Catalog test environment (Postgres + Unity + Spark) ---")
            ComposeContainer(File("$projectRoot/docker-compose.yml"))
                .withExposedService("postgres", 5432, 
                    Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(60)))
                .withExposedService("unity", 8080, 
                    Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(120)))
                .withExposedService("spark", 15002, 
                    Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(180)))
                .withLocalCompose(true)
                .apply { start() }
        }

        // Lazy initialization of the SparkSession
        val spark: SparkSession by lazy {
            val sparkPort = composeContainer.getServicePort("spark", 15002)
            
            val session = SparkSession.builder()
                .remote("sc://localhost:${sparkPort}")
                .getOrCreate()

            val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-test-fat.jar")
            
            if (jarFile.exists()) {
                session.addArtifact(jarFile.toURI())
            } else {
                throw IllegalStateException("Test fat JAR not found at ${jarFile.absolutePath}. Please run the 'testFatJar' task.")
            }
            session
        }
        
        /**
         * Get the Unity Catalog port for REST API calls.
         * Must be called after containers are started.
         */
        fun getUnityCatalogPort(): Int {
            return composeContainer.getServicePort("unity", 8080)
        }

        @AfterAll
        @JvmStatic
        fun teardown() {
            // Do not stop the session here. Let Testcontainers manage the lifecycle.
        }
    }
}
