package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

public class DatasetAdvancedJavaTest extends JavaSparkTestBase {

    // --- 7. Advanced/Other ---

    @Test
    public void testAlias() {
        Dataset<Row> aliased = peopleDF.as("people_alias");
        Dataset<Row> selected = aliased.select("people_alias.name");
        assertEquals("name", selected.columns()[0]);
    }

    @Test
    public void testCachePersistUnpersist() {
        assertDoesNotThrow(() -> {
            peopleDF.cache();
            assertTrue(peopleDF.storageLevel().useMemory());
            peopleDF.unpersist();
            
            peopleDF.persist(StorageLevel.MEMORY_ONLY());
            assertTrue(peopleDF.storageLevel().useMemory());
            peopleDF.unpersist();
        });
    }

    @Test
    public void testCheckpoint() {
        assertDoesNotThrow(() -> {
            Dataset<Row> localCheckpointed = peopleDF.localCheckpoint();
            assertEquals(4L, localCheckpointed.count());
        });
    }

    @Test
    public void testTempViews() {
        peopleDF.createOrReplaceTempView("people_view_java");
        Dataset<Row> fromView = spark.sql("SELECT * FROM people_view_java");
        assertEquals(4L, fromView.count());

        peopleDF.createOrReplaceGlobalTempView("people_global_view_java");
        Dataset<Row> fromGlobalView = spark.sql("SELECT * FROM global_temp.people_global_view_java");
        assertEquals(4L, fromGlobalView.count());
    }

    @Test
    public void testNaFunctions() {
        Dataset<Row> dfWithNulls = spark.sql("SELECT 'Alice' as name, 30 as age UNION ALL SELECT null as name, 40 as age");
        
        Dataset<Row> dropped = dfWithNulls.na().drop();
        assertEquals(1L, dropped.count());

        Dataset<Row> filled = dfWithNulls.na().fill("Unknown", new String[]{"name"});
        assertEquals("Unknown", filled.filter("age = 40").first().getAs("name"));
    }

    @Test
    public void testStatFunctions() {
        double[] approxQuantile = peopleDF.stat().approxQuantile("age", new double[]{0.5}, 0.0);
        assertEquals(1, approxQuantile.length);
        // Exact median (relativeError=0.0) of [25, 30, 35, 40] is one of the two middle values
        assertTrue(approxQuantile[0] >= 30.0 && approxQuantile[0] <= 35.0);
    }

    @Test
    public void testToJSON() {
        Dataset<String> jsonDS = peopleDF.toJSON();
        List<String> jsonList = jsonDS.collectAsList();
        assertEquals(4, jsonList.size());
        assertTrue(jsonList.stream().anyMatch(json -> json.contains("\"name\":\"Alice\"")));
    }

    @Test
    public void testTransform() {
        Dataset<Row> transformed = peopleDF.transform(df -> 
            df.withColumn("age_doubled", col("age").multiply(2))
        );
        assertEquals(60, (int) transformed.first().<Integer>getAs("age_doubled"));
    }
}
