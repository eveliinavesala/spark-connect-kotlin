package classes

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll

/**
 * Base class for all library tests (reflection engine, DSL, data integrity, etc.).
 *
 * Uses [SparkContainerManager] — a single bare Spark container started from the root
 * Dockerfile (`spark-server:latest`). Built and started via `make test`.
 *
 * For Unity Catalog integration tests, see [UnityCatalogTestBase].
 */
abstract class SparkTestBase {
    protected val spark: SparkSession
        get() = SparkContainerManager.sparkSession

    companion object {
        @AfterAll
        @JvmStatic
        fun teardown() {
            // Session lifecycle is managed by SparkContainerManager (singleton).
            // Do not stop the session here — other test classes share it.
        }
    }
}
