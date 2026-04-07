import org.gradle.api.file.DuplicatesStrategy
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.24"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

version = "1.0"

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
    testImplementation("org.testcontainers:testcontainers:1.21.4")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    
    // Delta Lake for Unity Catalog tests
    testImplementation("io.delta:delta-spark_2.13:3.2.0")
}

tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// ----------------------------------------------------------------------------
// Demo source sets
//
// "demo"     — exploration / thesis illustration code (app/, classes/, collections/)
//              Depends on main library output + implementation runtime deps.
//              Not included in any library artifact.
//
// "demoTest" — tests for demo code (Idiomatic*Test, SparkETLTest, etc.)
//              Depends on demo output + test output (for SparkTestBase) + test runtime deps.
//              Run with: ./gradlew demoTest
//              NOT included in ./gradlew test (library tests only).
// ----------------------------------------------------------------------------
sourceSets {
    create("demo") {
        kotlin.srcDir("src/demo/kotlin")
        compileClasspath += sourceSets.main.get().output + configurations.runtimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
    create("demoTest") {
        kotlin.srcDir("src/demo/test/kotlin")
        compileClasspath += sourceSets.main.get().output + sourceSets["demo"].output + sourceSets.test.get().output + configurations.testRuntimeClasspath.get()
        runtimeClasspath += output + compileClasspath
    }
}

tasks.register<Test>("demoTest") {
    description = "Runs demo/exploration tests separately from library tests."
    group = "verification"
    testClassesDirs = sourceSets["demoTest"].output.classesDirs
    classpath = sourceSets["demoTest"].runtimeClasspath
    useJUnitPlatform()
    dependsOn(testFatJar)
    System.getenv("DOCKER_HOST")?.let {
        environment("DOCKER_HOST", it)
    }
    jvmArgs(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
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
    jvmToolchain(21)
}
