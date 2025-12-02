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
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
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
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
}

kotlin {
    jvmToolchain(17)
}
