package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

public class DatasetTransformationsJavaTest extends JavaSparkTestBase {

    // --- 2. Basic Transformations ---

    @Test
    public void testFilterAndWhere() {
        Dataset<Row> filtered = peopleDF.filter(col("age").gt(30));
        assertEquals(2, filtered.count());

        Dataset<Row> filteredWithWhere = peopleDF.where("age > 30");
        assertEquals(2, filteredWithWhere.count());
    }

    @Test
    public void testSelectAndSelectExpr() {
        Dataset<Row> selected = peopleDF.select("name");
        assertEquals(1, selected.columns().length);
        assertEquals("name", selected.columns()[0]);

        Dataset<Row> selectedExpr = peopleDF.selectExpr("age + 1 as new_age");
        assertEquals(31, selectedExpr.first().getInt(0));
    }

    @Test
    public void testDrop() {
        Dataset<Row> dropped = peopleDF.drop("age");
        assertFalse(Arrays.asList(dropped.columns()).contains("age"));
        assertTrue(Arrays.asList(dropped.columns()).contains("name"));
    }

    @Test
    public void testWithColumnAndWithColumnRenamed() {
        Dataset<Row> withNewCol = peopleDF.withColumn("age_plus_5", col("age").plus(5));
        assertEquals(35, withNewCol.first().getInt(3));

        Dataset<Row> renamed = peopleDF.withColumnRenamed("age", "years");
        assertTrue(Arrays.asList(renamed.columns()).contains("years"));
        assertFalse(Arrays.asList(renamed.columns()).contains("age"));
    }

    @Test
    public void testDistinctAndDropDuplicates() {
        List<Person> duplicateData = Arrays.asList(
            new Person("Alice", 30, 1),
            new Person("Bob", 40, 2),
            new Person("Alice", 30, 1)
        );
        Dataset<Row> dfWithDupes = spark.createDataFrame(duplicateData, Person.class);
        
        assertEquals(3, dfWithDupes.count());
        assertEquals(2, dfWithDupes.distinct().count());
        assertEquals(2, dfWithDupes.dropDuplicates().count());
    }

    @Test
    public void testLimitAndOffset() {
        assertEquals(2, peopleDS.limit(2).count());
        assertEquals(2, peopleDS.offset(2).count());
        assertEquals("Charlie", peopleDS.orderBy("name").offset(2).first().getName());
    }

    @Test
    public void testOrderByAndSort() {
        List<Person> sortedList = peopleDS.orderBy(col("age")).collectAsList();
        assertEquals("Charlie", sortedList.get(0).getName());
        assertEquals("Alice", sortedList.get(1).getName());

        List<Person> sortedDescList = peopleDS.sort(col("name").desc()).collectAsList();
        assertEquals("David", sortedDescList.get(0).getName());
    }

    @Test
    public void testSample() {
        // Spark Connect 4.0.0 deterministic RNG produces 3 for this seed
        long count = peopleDS.sample(false, 0.5, 123L).count();
        assertEquals(3, count);
    }
}
