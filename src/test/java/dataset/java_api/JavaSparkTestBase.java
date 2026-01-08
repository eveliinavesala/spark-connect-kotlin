package dataset.java_api;

import classes.SparkContainerManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.List;

public class JavaSparkTestBase {

    protected static SparkSession spark;
    protected Dataset<Row> peopleDF;
    protected Dataset<Person> peopleDS;
    protected List<Person> data;

    @BeforeAll
    public static void setupSpark() {
        // Get the singleton session from the Kotlin manager
        spark = SparkContainerManager.INSTANCE.getSparkSession();
    }

    @BeforeEach
    public void setupData() {
        data = Arrays.asList(
            new Person("Alice", 30, 1),
            new Person("Bob", 40, 2),
            new Person("Charlie", 25, 1),
            new Person("David", 35, 3)
        );
        
        peopleDS = spark.createDataset(data, Encoders.bean(Person.class));
        peopleDF = peopleDS.toDF();
    }
}
