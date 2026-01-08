package dataset.java_api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.junit.jupiter.api.Assertions.*;

public class DatasetMetadataJavaTest extends JavaSparkTestBase {

    // --- 6. Metadata and Schema ---

    @Test
    public void testSchema() {
        StructType schema = peopleDF.schema();
        
        assertEquals(3, schema.fields().length);
        Map<String, org.apache.spark.sql.types.DataType> schemaMap = Arrays.stream(schema.fields())
            .collect(Collectors.toMap(f -> f.name(), f -> f.dataType()));

        assertEquals(DataTypes.StringType, schemaMap.get("name"));
        assertEquals(DataTypes.IntegerType, schemaMap.get("age"));
        assertEquals(DataTypes.IntegerType, schemaMap.get("cityId"));
    }

    @Test
    public void testDtypes() {
        scala.Tuple2<String, String>[] dtypes = peopleDF.dtypes();
        
        assertEquals(3, dtypes.length);
        Map<String, String> dtypeMap = Arrays.stream(dtypes)
            .collect(Collectors.toMap(t -> t._1(), t -> t._2()));

        // Spark Connect returns full type names
        assertEquals("StringType", dtypeMap.get("name"));
        assertEquals("IntegerType", dtypeMap.get("age"));
        assertEquals("IntegerType", dtypeMap.get("cityId"));
    }

    @Test
    public void testColumns() {
        String[] columns = peopleDF.columns();
        List<String> columnList = Arrays.asList(columns);
        assertEquals(3, columnList.size());
        assertTrue(columnList.contains("name"));
        assertTrue(columnList.contains("age"));
        assertTrue(columnList.contains("cityId"));
    }

    @Test
    public void testPrintSchema() {
        System.out.println("--- Java: Testing printSchema() ---");
        assertDoesNotThrow(() -> peopleDF.printSchema());
    }

    @Test
    public void testExplain() {
        Dataset<Row> filteredDF = peopleDF.filter("age > 30");
        
        System.out.println("--- Java: Testing explain() ---");
        assertDoesNotThrow(() -> filteredDF.explain(true));
    }

    @Test
    public void testInputFiles() {
        String[] files = peopleDF.inputFiles();
        assertEquals(0, files.length);
    }
}
