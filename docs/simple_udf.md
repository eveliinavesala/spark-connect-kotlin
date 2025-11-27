# Summary of UDF Success with Spark Connect

This document summarizes the successful implementation and testing of User-Defined Functions (UDFs) with idiomatic Kotlin code in a Spark Connect environment.

## 1. The Initial Problem: `ClassNotFoundException`

Our first attempt to use a UDF, even a simple one, failed with a `java.lang.ClassNotFoundException`.

- **Test:** `pragmatic approach can register and use a simple UDF()`
- **Error:** `ClassNotFoundException: Failed to load class: pragmatic.PragmaticUDFTest`
- **Root Cause:** This error perfectly demonstrated the client-server architecture of Spark Connect.
    1. Our UDF was defined as a lambda inside our test class on the **client**.
    2. The Spark Connect client serialized this lambda and sent it to the **server** for execution.
    3. The server's JVM did not have our project's test classes on its classpath and therefore could not deserialize or execute the lambda, causing the crash.

## 2. The Solution: Artifacts and the "Fat JAR"

The error message itself pointed to the solution: `Make sure the artifact where the class is defined is installed by calling session.addArtifact`. This led us to the correct, industry-standard approach for distributed applications.

1.  **Creating a "Fat JAR":** We configured our Gradle build (`build.gradle.kts`) to create a "fat JAR" (or "uber JAR"). This is a single JAR file that contains not only our own compiled Kotlin code but also all the code from our project's dependencies (like Testcontainers, JUnit, etc.).

    ```kotlin
    // build.gradle.kts
    tasks.jar {
        archiveClassifier.set("fat")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        from(sourceSets.main.get().output)
        from(sourceSets.test.get().output)
        from({
            configurations.testRuntimeClasspath.get().filter { it.name.endsWith(".jar") }.map { zipTree(it) }
        })
    }
    ```

2.  **Adding the Artifact to the Session:** We modified our `SparkTestBase.kt` to automatically find this fat JAR and send it to the Spark server every time a test session is created.

    ```kotlin
    // SparkTestBase.kt
    val jarFile = File(projectDir, "build/libs/spark-connect-kotlin-1.0-fat.jar")
    if (jarFile.exists()) {
        spark.addArtifact(jarFile.toURI())
    }
    ```

## 3. The Result: A Major Breakthrough

This two-part solution completely resolved the `ClassNotFoundException`. The Spark server now has all the code it needs to execute our custom Kotlin logic.

We successfully validated this with two common UDF patterns:

1.  **Named UDFs:** Using `spark.udf.register()` to create a named function that can be used in SQL expressions (`selectExpr`).
2.  **Anonymous UDFs:** Using the `udf()` factory from `org.apache.spark.sql.functions` to create a UDF that can be applied programmatically to a `Column` in a `withColumn` transformation.

Both tests passed, proving that our setup is robust and handles real-world UDF scenarios correctly.

## 4. Key Learnings

- The client and server in Spark Connect are **separate processes** with separate classpaths.
- Any custom code (UDFs, lambdas, custom classes) intended for execution on the server **must be explicitly sent** to the server.
- The standard mechanism for this is to package the client-side code and all its dependencies into a **fat JAR** and add it to the Spark session using `spark.addArtifact()`.

This successful implementation of UDFs is a critical milestone. It proves that we can build and run complex, idiomatic Kotlin ETL/ELT logic in a modern, distributed Spark environment.
