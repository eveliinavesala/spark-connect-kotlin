package unitycatalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import spark.kotlin.reflect.getSparkSchema
import kotlin.reflect.typeOf

/**
 * Utilities for integrating Kotlin data models with Unity Catalog (via Spark SQL).
 * This allows Kotlin data classes to serve as the canonical source of truth for table schemas.
 */
object UnityCatalogIntegrator {
    /**
     * Generates a Spark SQL `CREATE TABLE` statement based on the schema inferred from [T].
     * This is useful for previewing the schema or executing DDL directly.
     *
     * @param tableName The full table name (e.g., "catalog.schema.table").
     * @param format The table format (default: "DELTA").
     * @param location Optional external location (s3://...).
     */
    inline fun <reified T : Any> generateCreateSQL(
        tableName: String,
        format: String = "DELTA",
        location: String? = null,
    ): String {
        val schema = getSparkSchema(typeOf<T>())
        return buildCreateStatement(tableName, schema, format, location)
    }

    /**
     * Registers an empty table in Unity Catalog with the schema derived from [T].
     * If the table exists, this operation might fail or do nothing depending on `ignoreIfExists`.
     *
     * @param spark The active SparkSession.
     * @param tableName The full table name.
     * @param format The table format (default: "DELTA").
     * @param location Optional external location.
     * @param ignoreIfExists If true, uses `IF NOT EXISTS`.
     */
    inline fun <reified T : Any> registerTable(
        spark: SparkSession,
        tableName: String,
        format: String = "DELTA",
        location: String? = null,
        ignoreIfExists: Boolean = true,
    ) {
        val sql =
            generateCreateSQL<T>(tableName, format, location)
                .replace("CREATE TABLE", if (ignoreIfExists) "CREATE TABLE IF NOT EXISTS" else "CREATE TABLE")

        spark.sql(sql)
    }

    // Internal helper to build the SQL string
    fun buildCreateStatement(
        tableName: String,
        schema: StructType,
        format: String,
        location: String?,
    ): String {
        val columns =
            schema.fields().joinToString(",\n  ") { field ->
                val type = field.dataType().sql()
                val nullable = if (field.nullable()) "" else " NOT NULL"
                val comment =
                    if (field.metadata().contains("comment")) {
                        " COMMENT '${
                            field.metadata().getString("comment")
                        }'"
                    } else {
                        ""
                    }
                "${field.name()} $type$nullable$comment"
            }

        val locationClause = if (location != null) " LOCATION '$location'" else ""

        return """
            CREATE TABLE $tableName (
              $columns
            ) USING $format$locationClause
            """.trimIndent()
    }
}
