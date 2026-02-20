# Unity Catalog Integration - REST API Approach

## Overview

This project integrates Unity Catalog with Spark Connect using a **REST API-only approach**. The Unity Catalog Spark connector is NOT used due to incompatible dependencies with Spark 4.0.

## ⚠️ Important: Why REST API Only?

### Spark Connector Incompatibility

We attempted to use the Unity Catalog Spark connector (`unitycatalog-spark`) but encountered critical dependency conflicts:

1. **ANTLR Version Conflict**: Delta Lake 3.2.0 requires ANTLR 4.9.3 (ATN version 3), but Spark 4.0.0 ships with ANTLR 4.13.1 (ATN version 4). These versions are incompatible.
   
2. **Error Encountered**: 
   ```
   java.io.InvalidClassException: org.antlr.v4.runtime.atn.ATN; 
   Could not deserialize ATN with version 3 (expected 4)
   ```

3. **Solution Attempted**: Prepending ANTLR 4.9.3 to the classpath broke ALL Spark functionality (104+ test failures).

4. **Final Decision**: Use Unity Catalog's REST API exclusively, which works perfectly and has no dependency conflicts.

### Benefits of REST API Approach

✅ No dependency conflicts with Spark 4.0  
✅ Clean separation of concerns  
✅ Easier to test and debug  
✅ Full control over Unity Catalog operations  
✅ Compatible with any Spark version  

## Architecture

```
┌─────────────────┐
│   PostgreSQL    │ :5432  (Persistent metadata storage)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Unity Catalog   │ :8080  (Official image: unitycatalog/unitycatalog:latest)
│    Server       │        REST API: /api/2.1/unity-catalog/
└────────┬────────┘
         │
         │ (REST API calls only - no Spark connector)
         │
         ▼
┌─────────────────┐
│  Spark Connect  │ :15002 (Vanilla Spark 4.0 - NO Unity Catalog JARs)
│  (Plain)        │
└─────────────────┘
```

## What Was Built

### Docker Infrastructure
1. **docker/unity-catalog/hibernate.properties** - PostgreSQL configuration for Unity Catalog
2. **docker/spark/Dockerfile.spark-unity** - Plain Spark 4.0 (no Unity Catalog connector)
3. **docker/spark/spark-defaults.conf** - Minimal Spark configuration
4. **docker-compose.yml** - Full stack orchestration (PostgreSQL + Unity Catalog + Spark)

### Code Implementation
1. **src/main/kotlin/unity_catalog/UnityCatalogIntegrator.kt** - Type-safe table registration utilities
2. **src/test/kotlin/unity_catalog/UnityCatalogRestClient.kt** - REST API client for Unity Catalog operations
3. **src/test/kotlin/unity_catalog/UnityCatalogIntegrationTest.kt** - Complete integration tests using REST API
4. **src/test/kotlin/classes/SparkTestBase.kt** - Testcontainers support with ComposeContainer

### Test Status ✅

All 158 tests passing (153 original + 6 Unity Catalog tests - 1 unit test)

## Unity Catalog REST API

### Base URL
```
http://localhost:8080/api/2.1/unity-catalog
```

### Available Endpoints

#### Catalogs
- `POST /catalogs` - Create catalog
- `GET /catalogs` - List all catalogs
- `GET /catalogs/{name}` - Get catalog details

#### Schemas
- `POST /schemas` - Create schema
- `GET /schemas?catalog_name=X` - List schemas in a catalog
- `GET /schemas/{full_name}` - Get schema details

#### Tables
- `POST /tables` - Create table (see important note below)
- `GET /tables?catalog_name=X&schema_name=Y` - List tables in a schema
- `GET /tables/{full_name}` - Get table metadata

### Important: Table Creation Requirements

Unity Catalog REST API requires specific fields for table creation:

1. **Table Type**: Must be `EXTERNAL` (MANAGED tables require `server.managed-table.enabled=true` in server.properties)
2. **Column Fields Required**:
   - `name` - Column name
   - `type_text` - Data type as text (e.g., "INT", "STRING", "DOUBLE")
   - `type_name` - Same as type_text
   - `type_json` - JSON representation of the type
   - `nullable` - Boolean
   - `position` - Integer position (0-indexed)

Example table creation JSON:
```json
{
  "name": "my_table",
  "catalog_name": "my_catalog",
  "schema_name": "my_schema",
  "table_type": "EXTERNAL",
  "data_source_format": "DELTA",
  "storage_location": "/tmp/data/my_table",
  "columns": [
    {
      "name": "id",
      "type_text": "INT",
      "type_name": "INT",
      "type_json": "{\"name\":\"INT\",\"type\":\"INT\",\"nullable\":false,\"metadata\":{}}",
      "nullable": false,
      "position": 0
    }
  ]
}
```

## Usage

### Start the Stack
```bash
make uc-start
```

### Check Status
```bash
make uc-status
```

Expected output:
```
NAME                                IMAGE                                STATUS
spark-connect-kotlin-postgres-1     postgres:15                          Up (healthy)
spark-connect-kotlin-unity-1        unitycatalog/unitycatalog:latest     Up (healthy)
spark-connect-kotlin-spark-1        spark-connect-kotlin-spark           Up
```

### Using the REST API Client

```kotlin
import unity_catalog.UnityCatalogRestClient

val baseUrl = "http://localhost:8080"

// Create catalog
UnityCatalogRestClient.createCatalog(baseUrl, "my_catalog", "My test catalog")

// Create schema
UnityCatalogRestClient.createSchema(baseUrl, "my_catalog", "my_schema", "My schema")

// List catalogs
val catalogs = UnityCatalogRestClient.listCatalogs(baseUrl)

// Create table
val columns = listOf(
    mapOf("name" to "id", "type_name" to "INT", "nullable" to false, "position" to 0),
    mapOf("name" to "name", "type_name" to "STRING", "nullable" to false, "position" to 1)
)

UnityCatalogRestClient.createTable(
    baseUrl = baseUrl,
    catalogName = "my_catalog",
    schemaName = "my_schema",
    tableName = "my_table",
    columns = columns,
    storageLocation = "/tmp/data/my_table",
    dataSourceFormat = "DELTA"
)

// List tables
val tables = UnityCatalogRestClient.listTables(baseUrl, "my_catalog", "my_schema")

// Get table metadata
val tableInfo = UnityCatalogRestClient.getTable(baseUrl, "my_catalog.my_schema.my_table")
```

### Using UnityCatalogIntegrator (Type-Safe)

```kotlin
import unity_catalog.UnityCatalogIntegrator

data class Product(val id: Int, val name: String, val price: Double)

// Generate CREATE TABLE SQL from Kotlin data class
val sql = UnityCatalogIntegrator.generateCreateSQL<Product>(
    tableName = "my_catalog.my_schema.products",
    format = "DELTA"
)

// Register table schema with Spark (after creating via REST API)
val spark = SparkSession.builder()
    .remote("sc://localhost:15002")
    .getOrCreate()

UnityCatalogIntegrator.registerTable<Product>(
    spark = spark,
    tableName = "my_catalog.my_schema.products",
    format = "DELTA"
)
```

## Running Tests

### All Tests
```bash
./gradlew test
```

### Unity Catalog Tests Only
```bash
./gradlew test --tests "*UnityCatalog*"
```

### Integration Tests with Detailed Output
```bash
make uc-stacktrace
```

## Configuration

### PostgreSQL
- Image: `postgres:15`
- Database: `unity`
- User: `unity`
- Password: `unitypass`
- Port: 5432 (internal), mapped to random port in tests
- Data persists in Docker volume

### Unity Catalog
- Image: `unitycatalog/unitycatalog:latest`
- Port: 8080 (internal), mapped to random port in tests
- API: `http://localhost:8080/api/2.1/unity-catalog/`
- Backend: PostgreSQL (configured via hibernate.properties)
- Authentication: Disabled (development mode)
- Table Type: EXTERNAL tables only (MANAGED tables disabled)

### Spark Connect
- Base: Spark 4.0.0 (vanilla, no Unity Catalog connector)
- Port: 15002 (internal), mapped to random port in tests
- Spark UI: 4040
- Configuration: Minimal (adaptive execution only)

## Available Make Commands

```bash
# Unity Catalog lifecycle
make uc-build          # Build all images
make uc-start          # Build and start (recommended)
make uc-up             # Start services (if already built)
make uc-down           # Stop services
make uc-clean          # Stop and remove all data
make uc-restart        # Restart services

# Monitoring
make uc-status         # Check health of all services
make uc-logs           # View all logs
make uc-logs-postgres  # View PostgreSQL logs
make uc-logs-unity     # View Unity Catalog logs
make uc-logs-spark     # View Spark logs

# Testing
make uc-test           # Run tests with UC stack
make uc-stacktrace     # Run tests with detailed output

# Legacy (without Unity Catalog)
make build             # Build simple Spark image
make run               # Run simple Spark container
make test              # Run tests with simple setup
```

## Manual Testing

### 1. Test Unity Catalog API

Start the stack:
```bash
make uc-start
```

List catalogs:
```bash
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
# Should return: {"catalogs":[],"next_page_token":null}
```

Create a catalog:
```bash
curl -X POST http://localhost:8080/api/2.1/unity-catalog/catalogs \
  -H "Content-Type: application/json" \
  -d '{"name":"test_catalog","comment":"Test catalog"}'
```

### 2. Verify PostgreSQL Persistence

```bash
docker exec spark-connect-kotlin-postgres-1 \
  psql -U unity -d unity -c "SELECT * FROM uc_catalogs;"
```

## Troubleshooting

### Container won't start
```bash
make uc-logs
# Check specific service logs
```

### Port already in use
```bash
# Check what's using the ports
lsof -i :5432  # PostgreSQL
lsof -i :8080  # Unity Catalog
lsof -i :15002 # Spark
```

### Reset everything
```bash
make uc-clean  # Removes all containers and volumes
make uc-start  # Fresh start
```

### Verify PostgreSQL connection
```bash
docker exec spark-connect-kotlin-postgres-1 psql -U unity -d unity -c "\\dt"
```

### Table creation fails with "MANAGED table" error
This is expected. Use `table_type: "EXTERNAL"` instead. To enable MANAGED tables, add this to Unity Catalog's server.properties:
```
server.managed-table.enabled=true
```

## Next Steps for Production Use

1. **Enable Authentication**: Set `server.authorization=enable` in Unity Catalog server.properties
2. **External Storage**: Configure S3/ADLS credentials for table storage locations
3. **Volume Configuration**: Update docker-compose.yml with production volume paths
4. **Network Security**: Add network policies and firewall rules
5. **Monitoring**: Add Prometheus/Grafana for metrics
6. **Backup Strategy**: Implement PostgreSQL backup/restore procedures
7. **Consider MANAGED Tables**: Enable `server.managed-table.enabled=true` if needed

## Technical Notes

### Why Not Use the Spark Connector?

The Unity Catalog Spark connector (`unitycatalog-spark_2.13`) integrates Unity Catalog directly into Spark's catalog layer, allowing you to use Unity Catalog tables seamlessly via Spark SQL. However:

1. It requires Delta Lake as a dependency
2. Delta Lake 3.2.0 depends on ANTLR 4.9.3
3. Spark 4.0.0 uses ANTLR 4.13.1
4. ANTLR versions are incompatible at the bytecode level (ATN serialization format changed)
5. Downgrading ANTLR breaks core Spark functionality

### REST API Limitations

The REST API approach works well for:
- Creating and managing catalog metadata (catalogs, schemas, tables)
- Type-safe schema generation from Kotlin data classes
- Integration testing

However, it does NOT provide:
- Automatic Spark SQL integration (you can't do `SELECT * FROM unity_catalog.schema.table` directly)
- Delta Lake sharing protocol integration
- Built-in access control enforcement in Spark queries

For these features, you would need the Spark connector, which requires resolving the ANTLR conflict (possibly by waiting for Spark/Delta Lake updates).

## Files Modified

- `docker-compose.yml` - Full Unity Catalog stack
- `Makefile` - UC lifecycle commands
- `build.gradle.kts` - Testcontainers version fix
- `src/test/kotlin/classes/SparkTestBase.kt` - ComposeContainer integration
- `README.md` - Documentation

## Files Created

- `docker/unity-catalog/hibernate.properties`
- `docker/spark/Dockerfile.spark-unity`
- `docker/spark/spark-defaults.conf`
- `src/main/kotlin/unity_catalog/UnityCatalogIntegrator.kt`
- `src/test/kotlin/unity_catalog/UnityCatalogRestClient.kt`
- `src/test/kotlin/unity_catalog/UnityCatalogIntegrationTest.kt`
- `src/test/kotlin/unity_catalog/UnityCatalogTest.kt`
- `docs/UNITY_CATALOG_SETUP.md` (this file)
