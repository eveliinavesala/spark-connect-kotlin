import org.gradle.api.file.DuplicatesStrategy
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.shadow)
}

version = "1.0"

dependencies {
    constraints {
        implementation(libs.commons.lang3) {
            because("Address security vulnerability in older versions of commons-lang3")
        }
        testImplementation(libs.commons.compress) {
            because("Address security vulnerabilities CVE-2024-25710 and CVE-2024-26308")
        }
    }

    implementation(libs.spark.connect.client) {
        exclude(group = "org.slf4j", module = "jul-to-slf4j")
    }
    implementation(libs.kotlin.reflect)
    implementation(libs.kotlinx.datetime)
    implementation(libs.kotlinx.serialization.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.testcontainers.core)
    testImplementation(libs.testcontainers.junit.jupiter)
    testRuntimeOnly(libs.junit.jupiter.engine)

    // Delta Lake for Unity Catalog tests
    testImplementation(libs.delta.lake)
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
        exclude(dependency("org.apache.spark:.*"))
        exclude(dependency("org.scala-lang:.*"))
        exclude(dependency("org.apache.arrow:.*"))
        exclude(dependency("org.apache.hadoop:.*"))
        exclude(dependency("io.netty:.*"))
        exclude(dependency("com.google.protobuf:.*"))
        exclude(dependency("org.slf4j:.*"))
        exclude(dependency("log4j:.*"))
        exclude(dependency("org.apache.logging.log4j:.*"))
        exclude(dependency("org.testcontainers:.*"))
        exclude(dependency("org.junit.jupiter:.*"))
        exclude(dependency("org.junit.platform:.*"))
        exclude(dependency("org.opentest4j:.*"))
    }
    mergeServiceFiles()
}

tasks.test {
    dependsOn(testFatJar)
    useJUnitPlatform()
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
