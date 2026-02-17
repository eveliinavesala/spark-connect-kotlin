import org.gradle.api.file.DuplicatesStrategy
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.24"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

version = "1.0"

application {
    mainClass.set("app.kotlin_spark.MainKt")
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

repositories {
    mavenCentral()
}

dependencies {
    constraints {
        implementation("org.apache.commons:commons-lang3:3.18.0") {
            because("Address security vulnerability in older versions of commons-lang3")
        }
        testImplementation("org.apache.commons:commons-compress:1.26.0") {
            because("Address security vulnerabilities CVE-2024-25710 and CVE-2024-26308")
        }
    }

    implementation("org.apache.spark:spark-connect-client-jvm_2.13:4.0.0") {
        exclude(group = "org.slf4j", module = "jul-to-slf4j")
    }
    implementation(kotlin("reflect"))
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.0")

    testImplementation(kotlin("test"))
    testImplementation("org.testcontainers:testcontainers:2.0.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
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
    
    // Pass DOCKER_HOST to the test process if it exists
    System.getenv("DOCKER_HOST")?.let {
        environment("DOCKER_HOST", it)
    }
    
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
}

kotlin {
    jvmToolchain(17)
}
