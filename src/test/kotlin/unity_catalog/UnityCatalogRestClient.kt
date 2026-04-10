package unity_catalog

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlinx.serialization.json.*

/**
 * Simple REST client for Unity Catalog API operations.
 * Used in tests to interact with Unity Catalog via REST API.
 */
object UnityCatalogRestClient {
    private val client = HttpClient.newBuilder().build()
    
    /**
     * Create a catalog in Unity Catalog via REST API
     */
    fun createCatalog(
        baseUrl: String,
        name: String,
        comment: String = ""
    ): Boolean {
        val requestBody = """
            {
                "name": "$name",
                "comment": "$comment"
            }
        """.trimIndent()
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/catalogs"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build()
        
        return try {
            println("Creating catalog '$name' at $baseUrl")
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            println("Response status: ${response.statusCode()}, body: ${response.body()}")
            response.statusCode() == 200 || response.statusCode() == 201
        } catch (e: Exception) {
            println("Failed to create catalog '$name': ${e.message}")
            e.printStackTrace()
            false
        }
    }
    
    /**
     * List all catalogs
     */
    fun listCatalogs(baseUrl: String): List<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/catalogs"))
            .GET()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                extractNames(response.body(), "name")
            } else {
                emptyList()
            }
        } catch (e: Exception) {
            println("Failed to list catalogs: ${e.message}")
            emptyList()
        }
    }
    
    /**
     * Create a schema in Unity Catalog via REST API
     */
    fun createSchema(
        baseUrl: String,
        catalogName: String,
        schemaName: String,
        comment: String = ""
    ): Boolean {
        val requestBody = """
            {
                "name": "$schemaName",
                "catalog_name": "$catalogName",
                "comment": "$comment"
            }
        """.trimIndent()
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/schemas"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build()
        
        return try {
            println("Creating schema '$catalogName.$schemaName' at $baseUrl")
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            println("Response status: ${response.statusCode()}, body: ${response.body()}")
            response.statusCode() == 200 || response.statusCode() == 201
        } catch (e: Exception) {
            println("Failed to create schema '$catalogName.$schemaName': ${e.message}")
            e.printStackTrace()
            false
        }
    }
    
    /**
     * List schemas in a catalog
     */
    fun listSchemas(baseUrl: String, catalogName: String): List<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/schemas?catalog_name=$catalogName"))
            .GET()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                extractNames(response.body(), "name")
            } else {
                emptyList()
            }
        } catch (e: Exception) {
            println("Failed to list schemas: ${e.message}")
            emptyList()
        }
    }
    
    /**
     * Create a table via REST API
     */
    fun createTable(
        baseUrl: String,
        catalogName: String,
        schemaName: String,
        tableName: String,
        columns: List<Map<String, Any>>,
        storageLocation: String,
        dataSourceFormat: String = "DELTA"
    ): Boolean {
        val columnsJson = columns.joinToString(",\n") { col ->
            val typeName = col["type_name"] as String
            """
            {
                "name": "${col["name"]}",
                "type_text": "$typeName",
                "type_name": "$typeName",
                "type_json": "{\"name\":\"$typeName\",\"type\":\"$typeName\",\"nullable\":${col["nullable"]},\"metadata\":{}}",
                "nullable": ${col["nullable"]},
                "position": ${col["position"]}
            }
            """.trimIndent()
        }
        
        val requestBody = """
            {
                "name": "$tableName",
                "catalog_name": "$catalogName",
                "schema_name": "$schemaName",
                "table_type": "EXTERNAL",
                "data_source_format": "$dataSourceFormat",
                "columns": [$columnsJson],
                "storage_location": "$storageLocation"
            }
        """.trimIndent()
        
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/tables"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build()
        
        return try {
            println("Creating table '$catalogName.$schemaName.$tableName' at $baseUrl")
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            println("Response status: ${response.statusCode()}, body: ${response.body()}")
            // 409 ALREADY_EXISTS = table is registered (idempotent); count as success
            response.statusCode() == 200 || response.statusCode() == 201 || response.statusCode() == 409
        } catch (e: Exception) {
            println("Failed to create table: ${e.message}")
            e.printStackTrace()
            false
        }
    }
    
    /**
     * List tables in a schema
     */
    fun listTables(baseUrl: String, catalogName: String, schemaName: String): List<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/tables?catalog_name=$catalogName&schema_name=$schemaName"))
            .GET()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                extractNames(response.body(), "name")
            } else {
                emptyList()
            }
        } catch (e: Exception) {
            println("Failed to list tables: ${e.message}")
            emptyList()
        }
    }
    
    /**
     * Get table metadata
     */
    fun getTable(baseUrl: String, catalogName: String, schemaName: String, tableName: String): Map<String, String>? {
        val fullName = "$catalogName.$schemaName.$tableName"
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/tables/$fullName"))
            .GET()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                val body = response.body()
                mapOf(
                    "name" to extractFieldValue(body, "name"),
                    "catalog_name" to extractFieldValue(body, "catalog_name"),
                    "schema_name" to extractFieldValue(body, "schema_name")
                )
            } else {
                null
            }
        } catch (e: Exception) {
            println("Failed to get table: ${e.message}")
            null
        }
    }
    
    /**
     * Check if a catalog exists
     */
    fun catalogExists(baseUrl: String, name: String): Boolean {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/catalogs/$name"))
            .GET()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            response.statusCode() == 200
        } catch (e: Exception) {
            false
        }
    }
    
    /**
     * Delete a catalog (cleanup in tests)
     */
    fun deleteCatalog(baseUrl: String, name: String): Boolean {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/catalogs/$name?force=true"))
            .DELETE()
            .build()
        
        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            response.statusCode() == 200 || response.statusCode() == 204
        } catch (e: Exception) {
            println("Failed to delete catalog '$name': ${e.message}")
            false
        }
    }
    
    /**
     * Get the column definitions of a table as a list of maps.
     * Keys: "name" (String), "type_name" (String), "nullable" (Boolean), "position" (Int).
     * Returns empty list if the table does not exist or the request fails.
     */
    fun getTableColumns(
        baseUrl: String,
        catalogName: String,
        schemaName: String,
        tableName: String
    ): List<Map<String, Any>> {
        val fullName = "$catalogName.$schemaName.$tableName"
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/api/2.1/unity-catalog/tables/$fullName"))
            .GET()
            .build()

        return try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() != 200) return emptyList()
            val root = Json.parseToJsonElement(response.body()).jsonObject
            val columns = root["columns"]?.jsonArray ?: return emptyList()
            columns.map { elem ->
                val col = elem.jsonObject
                buildMap {
                    put("name",      col["name"]?.jsonPrimitive?.content ?: "")
                    put("type_name", col["type_name"]?.jsonPrimitive?.content ?: "STRING")
                    put("nullable",  col["nullable"]?.jsonPrimitive?.booleanOrNull ?: true)
                    put("position",  col["position"]?.jsonPrimitive?.intOrNull ?: 0)
                }
            }
        } catch (e: Exception) {
            println("Failed to get columns for $fullName: ${e.message}")
            emptyList()
        }
    }

    // Helper function to extract field values from JSON using regex
    private fun extractFieldValue(json: String, field: String): String {
        val pattern = """"$field"\s*:\s*"([^"]*)"""".toRegex()
        return pattern.find(json)?.groupValues?.get(1) ?: ""
    }
    
    // Helper function to extract list of name values from JSON
    private fun extractNames(json: String, field: String): List<String> {
        val pattern = """"$field"\s*:\s*"([^"]*)"""".toRegex()
        return pattern.findAll(json).map { it.groupValues[1] }.toList()
    }
}
