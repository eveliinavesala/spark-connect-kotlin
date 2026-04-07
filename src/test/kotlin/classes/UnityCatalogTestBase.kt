package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.testcontainers.containers.ComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.time.Duration

/**
 * Base class for Unity Catalog integration tests.
 *
 * Starts the full docker-compose stack (Postgres + Unity Catalog + Spark) defined in
 * `docker-compose.yml`. Built and started via `make uc-test`.
 *
 * Exposes [spark] connected to the compose Spark service, and [getUnityCatalogPort]
 * for REST API calls against the Unity Catalog service.
 *
 * For ordinary library tests that only need Spark, use [SparkTestBase] instead —
 * it starts a single lightweight container and does not pull in Postgres or Unity Catalog.
 */
abstract class UnityCatalogTestBase {

    protected val spark: SparkSession
        get() = Companion.spark

    companion object {
        private val projectRoot = System.getProperty("user.dir")

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

        val spark: SparkSession by lazy {
            val sparkPort = composeContainer.getServicePort("spark", 15002)

            val session = SparkSession.builder()
                .remote("sc://localhost:${sparkPort}")
                .getOrCreate()

            val jarFile = File(projectRoot, "build/libs/spark-connect-kotlin-1.0-test-fat.jar")

            if (jarFile.exists()) {
                session.addArtifact(jarFile.toURI())
            } else {
                throw IllegalStateException(
                    "Test fat JAR not found at ${jarFile.absolutePath}. Run the 'testFatJar' task."
                )
            }
            session
        }

        /**
         * Returns the host port mapped to Unity Catalog's port 8080.
         * Must only be called after the compose stack has started (i.e., after [spark] is accessed).
         */
        fun getUnityCatalogPort(): Int = composeContainer.getServicePort("unity", 8080)

        @AfterAll
        @JvmStatic
        fun teardown() {
            // Testcontainers manages compose container shutdown via its own shutdown hook.
        }
    }
}
