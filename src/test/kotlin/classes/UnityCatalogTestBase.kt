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
 * Two modes:
 *
 * **Managed mode** (default): testcontainers starts the full docker-compose stack
 * (Postgres + Unity Catalog + Spark) from `docker-compose.yml`.
 *
 * **External mode** (`UC_EXTERNAL=true` env var): connects directly to an already-running
 * stack. Use this when you start the stack manually with `make uc-start` before running tests.
 * Spark is expected on port 15002, Unity Catalog on port 8080.
 *
 * For ordinary library tests that only need Spark, use [SparkTestBase] instead.
 */
abstract class UnityCatalogTestBase {

    protected val spark: SparkSession
        get() = Companion.spark

    companion object {
        private val projectRoot = System.getProperty("user.dir")

        /** True when containers are already running externally (UC_EXTERNAL=true). */
        private val isExternal: Boolean
            get() = System.getenv("UC_EXTERNAL")?.lowercase() == "true"

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
            val sparkPort = if (isExternal) {
                println("--- UC_EXTERNAL=true: connecting to already-running stack on port 15002 ---")
                15002
            } else {
                composeContainer.getServicePort("spark", 15002)
            }

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
         * Returns the host port for Unity Catalog's REST API.
         * In external mode this is always 8080; in managed mode it is the port mapped by testcontainers.
         */
        fun getUnityCatalogPort(): Int =
            if (isExternal) 8080 else composeContainer.getServicePort("unity", 8080)

        @AfterAll
        @JvmStatic
        fun teardown() {
            // Testcontainers manages compose container shutdown via its own shutdown hook.
            // In external mode, the caller is responsible for stopping the stack.
        }
    }
}
