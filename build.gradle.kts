import org.gradle.api.file.DuplicatesStrategy
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    application
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.shadow)
}

version = "1.0"

application {
    mainClass.set("app.kotlin_spark.MainKt")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

dependencies {
    constraints {
        implementation(libs.commons.lang3) {
            because("Address security vulnerability in older versions of commons-lang3")
        }
        testImplementation(libs.commons.compress) {
            because("Address security vulnerabilities CVE-2024-25710 and CVE-2024-26308")
        }
    }

    implementation(libs.kotlin.reflect)
    implementation(libs.kotlinx.datetime)
    implementation(libs.kotlinx.serialization.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.spark.connect.client) {
        exclude(group = "org.slf4j", module = "jul-to-slf4j")
    }

    testImplementation(libs.kotlin.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// Standard "fat jar" for deployment (main sources only)
tasks.shadowJar {
    archiveClassifier.set("fat")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    dependencies {
        exclude(dependency("org.apache.spark:.*"))
        exclude(dependency("org.scala-lang:.*"))
    }
    mergeServiceFiles()
}

// Optimized "test fat jar" - includes test classes but excludes provided dependencies
val testFatJar = tasks.register<ShadowJar>("testFatJar") {
    archiveClassifier.set("test-fat")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    from(sourceSets.main.get().output)
    from(sourceSets.test.get().output)
    
    configurations = listOf(project.configurations.testRuntimeClasspath.get())
    
    dependencies {
        // Exclude dependencies already provided by the Spark server
        exclude(dependency("org.apache.spark:.*"))
        exclude(dependency("org.scala-lang:.*"))
        exclude(dependency("org.apache.arrow:.*"))
        exclude(dependency("org.apache.hadoop:.*"))
        exclude(dependency("io.netty:.*"))
        exclude(dependency("com.google.protobuf:.*"))
        exclude(dependency("org.slf4j:.*"))
        exclude(dependency("log4j:.*"))
        exclude(dependency("org.apache.logging.log4j:.*"))
        
        // Exclude test infrastructure not needed on the server
        exclude(dependency("org.testcontainers:.*"))
        exclude(dependency("org.junit.jupiter:.*"))
        exclude(dependency("org.junit.platform:.*"))
        exclude(dependency("org.opentest4j:.*"))
    }
    mergeServiceFiles()
}

tasks.test {
    // Ensure the test fat jar is built before tests are run
    dependsOn(testFatJar)
    useJUnitPlatform()
    
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
}

kotlin {
    jvmToolchain(21)
}
