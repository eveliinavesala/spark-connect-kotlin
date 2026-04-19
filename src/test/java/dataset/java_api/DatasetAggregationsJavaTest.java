package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

public class DatasetAggregationsJavaTest extends JavaSparkTestBase {

    private Dataset<Row> salesDF;

    @BeforeEach
    public void setupSales() {
        List<Row> salesData = Arrays.asList(
                org.apache.spark.sql.RowFactory.create("Java", "Helsinki", 100),
                org.apache.spark.sql.RowFactory.create("Kotlin", "Helsinki", 150),
                org.apache.spark.sql.RowFactory.create("Java", "Turku", 200),
                org.apache.spark.sql.RowFactory.create("Kotlin", "Turku", 250)
        );
        salesDF = spark.createDataFrame(salesData,
                new org.apache.spark.sql.types.StructType()
                        .add("course", "string")
                        .add("city", "string")
                        .add("sales", "integer")
        );
    }

    // --- 3. Aggregations ---

    @Test
    public void testGroupByAndAgg() {
        Dataset<Row> grouped = salesDF.groupBy("city").agg(sum("sales"), avg("sales"));

        Map<String, Row> results = grouped.collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getString(0), row -> row));

        Row helsinkiRow = results.get("Helsinki");
        assertNotNull(helsinkiRow);
        assertEquals(250, helsinkiRow.getLong(1));
        assertEquals(125.0, helsinkiRow.getDouble(2), 0.001);
    }

    @Test
    public void testCube() {
        Dataset<Row> cubed = salesDF.cube("city", "course").agg(sum("sales"));
        assertEquals(9, cubed.count());
    }

    @Test
    public void testRollup() {
        Dataset<Row> rolledUp = salesDF.rollup("city", "course").agg(sum("sales"));
        assertEquals(7, rolledUp.count());
    }

    @Test
    public void testSummaryAndDescribe() {
        Dataset<Row> summary = salesDF.summary("count", "mean");
        // The schema of summary can be complex, let's just check the row count
        assertEquals(2, summary.count());

        Dataset<Row> describe = salesDF.describe("sales");
        assertEquals(5, describe.count()); // count, mean, stddev, min, max
    }
}
