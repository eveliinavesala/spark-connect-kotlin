import com.google.protobuf.gradle.*
import org.gradle.api.file.DuplicatesStrategy

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.9.10"
    id("com.google.protobuf") version "0.9.4"
}

group = "org.jetbrains.kotlinx.spark"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven { url = uri("https://repository.apache.org/snapshots") }
}

dependencies {
    implementation("org.apache.spark:spark-connect-client_2.13:4.0.0-SNAPSHOT")
    implementation("com.google.protobuf:protobuf-kotlin:3.25.3")
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    testImplementation(kotlin("test"))
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    sourceSets {
        getByName("main") {
            proto {
                srcDir("src/main/proto")
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

tasks.processResources {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}