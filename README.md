# Spark Connect Kotlin Example

This project demonstrates a Kotlin application connecting to a Spark 4.0 cluster using Spark Connect, with full Unity
Catalog integration for table management and schema evolution.

## Prerequisites

* [Docker](https://www.docker.com/get-started) must be installed and running.
* Docker Compose (included with Docker Desktop).
* A Java 21 JDK.
* An IDE that supports Gradle projects (e.g., IntelliJ IDEA).

## Quick Start

### Option 1: Unity Catalog Stack (Recommended)

The full Unity Catalog stack includes PostgreSQL, Unity Catalog server, and Spark with Unity integration.

```sh
# Build and start all services
make uc-start

# Run your application
make app

# Run tests
make uc-test

# View logs
make uc-logs

# Stop services
make uc-down

# Clean up (including data volumes)
make uc-clean
```

### Option 2: Simple Spark Server

For basic Spark Connect without Unity Catalog:

```sh
# Build and run standalone Spark server
make build
make run

# Run application
make app

# Run tests
make test
```

## Unity Catalog Integration

This project includes a production-ready Unity Catalog setup with PostgreSQL backend for persistent metadata storage.
Integration uses Unity Catalog's **REST API** for catalog operations.

> **Note:** This implementation uses the Unity Catalog REST API instead of the Spark connector due to ANTLR dependency
> conflicts between Delta Lake 3.2.0 and Spark 4.0.0. See [docs/UNITY_CATALOG_SETUP.md](devdocs/UNITY_CATALOG_SETUP.md)
> for details.

### Architecture

The Unity Catalog stack consists of three services:

- **PostgreSQL** (port 5432): Persistent metadata storage
- **Unity Catalog** (port 8080): Catalog server with REST API
- **Spark Connect** (port 15002): Vanilla Spark 4.0 server (no Unity Catalog connector)

### Using Unity Catalog in Code

The project provides two ways to interact with Unity Catalog:

#### 1. REST API Client (Recommended for catalog operations)

```kotlin
import unity_catalog.UnityCatalogRestClient

val baseUrl = "http://localhost:8080"

// Create catalog and schema
UnityCatalogRestClient.createCatalog(baseUrl, "my_catalog", "Test catalog")
UnityCatalogRestClient.createSchema(baseUrl, "my_catalog", "my_schema", "Test schema")

// Create table with EXTERNAL type
val columns = listOf(
    mapOf("name" to "id", "type_name" to "INT", "nullable" to false, "position" to 0),
    mapOf("name" to "name", "type_name" to "STRING", "nullable" to false, "position" to 1),
    mapOf("name" to "price", "type_name" to "DOUBLE", "nullable" to false, "position" to 2)
)

UnityCatalogRestClient.createTable(
    baseUrl = baseUrl,
    catalogName = "my_catalog",
    schemaName = "my_schema",
    tableName = "products",
    columns = columns,
    storageLocation = "/tmp/data/products",
    dataSourceFormat = "DELTA"
)

// List and query
val tables = UnityCatalogRestClient.listTables(baseUrl, "my_catalog", "my_schema")
val tableInfo = UnityCatalogRestClient.getTable(baseUrl, "my_catalog.my_schema.products")
```

#### 2. Type-Safe Schema Generator

The `UnityCatalogIntegrator` provides type-safe SQL generation from Kotlin data classes:

```kotlin
import unity_catalog.UnityCatalogIntegrator

data class Product(val id: Int, val name: String, val price: Double)

// Generate CREATE TABLE SQL from data class
val sql = UnityCatalogIntegrator.generateCreateSQL<Product>(
    tableName = "my_catalog.my_schema.products",
    format = "DELTA"
)
println(sql)
// Output:
// CREATE TABLE my_catalog.my_schema.products (
//   id INT NOT NULL,
//   name STRING NOT NULL,
//   price DOUBLE NOT NULL
// ) USING DELTA
```

### Unity Catalog Commands

```sh
make uc-build        # Build all images
make uc-start        # Build and start services
make uc-up           # Start services
make uc-down         # Stop services
make uc-clean        # Stop and remove all data
make uc-restart      # Restart all services
make uc-logs         # Show all logs
make uc-logs-unity   # Show Unity Catalog logs
make uc-logs-spark   # Show Spark logs
make uc-logs-postgres # Show PostgreSQL logs
make uc-status       # Check service health
make uc-test         # Run tests with UC stack
```

## Running Tests

This project uses [Testcontainers](https://www.testcontainers.org/) to automatically manage the full Unity Catalog stack
for testing.

```sh
# Run all tests with Unity Catalog
make uc-test

# Run tests with detailed output
make uc-stacktrace
```

Tests automatically start PostgreSQL, Unity Catalog, and Spark containers, then run the full test suite including Unity
Catalog integration tests.
