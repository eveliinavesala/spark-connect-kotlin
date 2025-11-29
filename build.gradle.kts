import org.gradle.api.file.DuplicatesStrategy

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.24"
    application
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
    // Define a constraint to force a specific version of a transitive dependency.
    constraints {
        implementation("org.apache.commons:commons-lang3:3.18.0") {
            because("Address security vulnerability in older versions of commons-lang3")
        }
        testImplementation("org.apache.commons:commons-compress:1.26.0") {
            because("Address security vulnerabilities CVE-2024-25710 and CVE-2024-26308")
        }
    }

    // Main dependency
    implementation("org.apache.spark:spark-connect-client-jvm_2.13:4.0.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.9.24")
    
    // Test dependencies
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5") // Use the JUnit 5 variant of kotlin-test
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")

    // Explicitly declare the JUnit 5 engine to prevent Gradle's automatic loading deprecation warning.
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
}

tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.jar {
    archiveClassifier.set("fat")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from(sourceSets.main.get().output)
    from(sourceSets.test.get().output)
    from({
        configurations.testRuntimeClasspath.get().filter { it.name.endsWith(".jar") }.map { zipTree(it) }
    })
}

tasks.test {
    useJUnitPlatform()
    dependsOn(tasks.jar)
    
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}

kotlin {
    jvmToolchain(17)
}
