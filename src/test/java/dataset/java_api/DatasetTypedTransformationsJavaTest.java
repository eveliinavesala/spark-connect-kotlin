package dataset.java_api;

import org.junit.jupiter.api.Disabled;

@Disabled("Typed transformations using Java lambdas are not compatible with Spark Connect without complex UDF registration and fail with serialization errors.")
public class DatasetTypedTransformationsJavaTest extends JavaSparkTestBase {
    // This entire test class is disabled.
}
