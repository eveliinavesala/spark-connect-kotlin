import com.google.protobuf.gradle.*
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.tasks.JavaExec

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.10"
    id("com.google.protobuf") version "0.9.4"
}

group = "org.jetbrains.kotlinx.spark"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.spark:spark-connect-client-jvm_2.13:4.0.0")
    "protobuf"("org.apache.spark:spark-connect-client-jvm_2.13:4.0.0")
    implementation("com.google.protobuf:protobuf-kotlin:3.25.3")
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    testImplementation(kotlin("test"))
}

configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.zookeeper" && requested.name == "zookeeper") {
            useVersion("3.9.4")
            because("Addressing security vulnerability")
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    sourceSets {
        getByName("main") {
            proto {
                srcDir("src/main/proto")
                exclude("spark/**")
            }
        }
    }
    generateProtoTasks {
        all().forEach {
            it.builtins {
                kotlin { }
            }
        }
    }
}

sourceSets["main"].java.srcDir("build/generated/source/proto/main/kotlin")

tasks.withType<JavaExec> {
    jvmArgs("--add-opens=java.base/java.nio=ALL-UNNAMED")
}

tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}