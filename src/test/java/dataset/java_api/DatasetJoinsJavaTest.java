package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.*;

public class DatasetJoinsJavaTest extends JavaSparkTestBase {

    public static class City implements Serializable {
        private int id;
        private String name;

        public City() {}
        public City(int id, String name) { this.id = id; this.name = name; }
        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    private Dataset<Row> citiesDF;

    @BeforeEach
    public void setupJoins() {
        List<City> cityData = Arrays.asList(
            new City(1, "Helsinki"),
            new City(2, "Turku"),
            new City(4, "Stockholm")
        );
        citiesDF = spark.createDataFrame(cityData, City.class);
    }

    // --- 4. Joins and Set Operations ---

    @Test
    public void testJoin() {
        Dataset<Row> people = peopleDF.alias("people");
        Dataset<Row> cities = citiesDF.alias("cities");

        Dataset<Row> joined = people.join(cities, col("people.cityId").equalTo(col("cities.id")));
        assertEquals(3, joined.count());

        Dataset<Row> leftJoin = people.join(cities, col("people.cityId").equalTo(col("cities.id")), "left_outer");
        assertEquals(4, leftJoin.count());
        assertEquals(1, leftJoin.filter("cities.name is null").count());

        Dataset<Row> crossJoin = people.crossJoin(cities);
        assertEquals(4 * 3, crossJoin.count());
    }

    @Test
    public void testJoinWith() {
        Dataset<City> citiesDS = citiesDF.as(Encoders.bean(City.class));
        Dataset<Tuple2<Person, City>> joined = peopleDS.joinWith(citiesDS, peopleDS.col("cityId").equalTo(citiesDS.col("id")), "inner")
            .orderBy(col("_1.name")); // Add deterministic ordering
        
        List<Tuple2<Person, City>> results = joined.collectAsList();
        assertEquals(3, results.size());
        assertEquals("Alice", results.get(0)._1().getName());
        assertEquals("Helsinki", results.get(0)._2().getName());
    }

    @Test
    public void testUnionAndExcept() {
        Dataset<Row> morePeople = spark.createDataFrame(Arrays.asList(new Person("Eve", 28, 4)), Person.class);
        
        Dataset<Row> union = peopleDF.union(morePeople);
        assertEquals(5, union.count());

        Dataset<Row> subset = spark.createDataFrame(Arrays.asList(data.get(0), data.get(1)), Person.class);
        Dataset<Row> exception = peopleDF.except(subset);
        assertEquals(2, exception.count());
    }
}
