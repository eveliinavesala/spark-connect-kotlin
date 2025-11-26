package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class SparkTestBase {

    companion object {
        @Container
        @JvmField
        // Use the pre-built 'spark-server' image from the local Docker daemon.
        // This assumes `make build` or `docker build -t spark-server .` has been run.
        val sparkContainer: GenericContainer<*> = GenericContainer("spark-server")
            .withExposedPorts(15002)

        lateinit var spark: SparkSession

        @BeforeAll
        @JvmStatic
        fun setup() {
            spark = SparkSession.builder()
                .remote("sc://localhost:${sparkContainer.getMappedPort(15002)}")
                .getOrCreate()
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
