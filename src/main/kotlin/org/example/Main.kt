package org.example

import org.apache.spark.sql.SparkSession

fun main() {
    val spark = SparkSession.builder()
        .remote("sc://localhost")
        .getOrCreate()

    val df = spark.range(10)
    df.show()
}
