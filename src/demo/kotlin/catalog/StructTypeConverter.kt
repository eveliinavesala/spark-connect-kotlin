package catalog

import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

// ── StructType → UC columns ───────────────────────────────────────────────────

/**
 * Top-level extension functions for converting between Spark [StructType] and the Unity Catalog
 * REST API column format (list of maps with keys: name, type_name, nullable, position).
 *
 * These live in the demo layer — user-space utilities, not part of the library core.
 */

fun StructType.toUcColumns(): List<Map<String, Any>> =
    fields().mapIndexed { i, f ->
        mapOf(
            "name" to f.name(),
            "type_name" to sparkTypeToUcTypeName(f.dataType()),
            "nullable" to f.nullable(),
            "position" to i,
        )
    }

private fun sparkTypeToUcTypeName(dt: DataType): String =
    when (dt) {
        is StringType -> "STRING"
        is IntegerType -> "INT"
        is LongType -> "LONG"
        is DoubleType -> "DOUBLE"
        is FloatType -> "FLOAT"
        is BooleanType -> "BOOLEAN"
        is TimestampType -> "TIMESTAMP"
        is DateType -> "DATE"
        is BinaryType -> "BINARY"
        is ShortType -> "SHORT"
        is ByteType -> "BYTE"
        is DecimalType -> "DECIMAL"
        is ArrayType -> "ARRAY"
        is MapType -> "MAP"
        is StructType -> "STRUCT"
        else -> "STRING"
    }

// ── UC columns → StructType ───────────────────────────────────────────────────

fun List<Map<String, Any>>.toStructType(): StructType {
    val sorted = sortedBy { (it["position"] as? Int) ?: 0 }
    val fields =
        sorted.map { col ->
            val name = col["name"] as String
            val typeName = col["type_name"] as String
            val nullable = col["nullable"] as? Boolean ?: true
            DataTypes.createStructField(name, ucTypeNameToSparkType(typeName), nullable)
        }
    return StructType(fields.toTypedArray())
}

private fun ucTypeNameToSparkType(typeName: String): DataType =
    when (typeName.uppercase()) {
        "STRING", "VARCHAR", "CHAR" -> DataTypes.StringType
        "INT", "INTEGER" -> DataTypes.IntegerType
        "LONG", "BIGINT" -> DataTypes.LongType
        "DOUBLE" -> DataTypes.DoubleType
        "FLOAT", "REAL" -> DataTypes.FloatType
        "BOOLEAN", "BOOL" -> DataTypes.BooleanType
        "TIMESTAMP", "TIMESTAMP_NTZ" -> DataTypes.TimestampType
        "DATE" -> DataTypes.DateType
        "BINARY", "BYTES" -> DataTypes.BinaryType
        "SHORT", "SMALLINT" -> DataTypes.ShortType
        "BYTE", "TINYINT" -> DataTypes.ByteType
        "DECIMAL", "NUMERIC" -> DataTypes.createDecimalType(38, 18)
        else -> DataTypes.StringType
    }
