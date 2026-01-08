package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DatasetActionsJavaTest extends JavaSparkTestBase {

    // --- 1. Core Actions (Connect safe) ---

    @Test
    public void testCollectAndCollectAsList() {
        List<Person> collected = peopleDS.collectAsList();

        assertEquals(4, collected.size());
        // Order is not guaranteed in Connect, so only check membership
        assertTrue(collected.stream().anyMatch(p -> p.getName().equals("Alice")));
    }

    @Test
    public void testCount() {
        assertEquals(4L, peopleDF.count());
    }

    @Test
    public void testFirstAndHead() {
        Person f = peopleDS.first();
        Person h = peopleDS.head();

        // Connect guarantees both operations behave identically
        assertEquals(f, h);
        assertNotNull(f.getName());
    }

    @Test
    public void testTakeAndTakeAsList() {
        Object[] arr = (Object[]) peopleDS.take(2);
        List<Person> list = peopleDS.takeAsList(2);

        assertEquals(2, arr.length);
        assertEquals(2, list.size());
    }

    @Test
    public void testTail() {
        Object[] tail = (Object[]) peopleDS.tail(2);
        assertEquals(2, tail.length);
    }

    @Test
    public void testIsEmpty() {
        Dataset<Person> empty = spark.emptyDataset(Encoders.bean(Person.class));
        assertTrue(empty.isEmpty());
        assertFalse(peopleDS.isEmpty());
    }

    @Test
    public void testShow() {
        assertDoesNotThrow(() -> peopleDF.show());
    }

    // === Connect-incompatible operations removed ===
    // foreach(), foreachPartition(), map(), flatMap(), mapPartitions(), reduce(JavaFunction)
    // They cannot work through Spark Connect without complex UDF registration.

    @Test
    public void testSumAgeWithSqlExpression() {
        // Equivalent of reduce but Connect-compatible
        org.apache.spark.sql.Row result = peopleDF.selectExpr("sum(age) as total").collectAsList().get(0);

        assertEquals(130L, result.getLong(0));
    }
}
