plugins {
    java
    id("io.quarkus")
    id("io.freefair.lombok") version "8.3"
}

repositories {
    mavenCentral()
    mavenLocal()
    maven   {
     url = uri("https://packages.confluent.io/maven/")
    }
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))
    implementation("io.quarkus:quarkus-confluent-registry-avro")
    // cflt SR libs use Jakarta REST client
    implementation("io.quarkus:quarkus-rest-client-reactive")
    implementation("io.confluent:kafka-streams-avro-serde:7.5.1")
    implementation("io.confluent:kafka-avro-serializer:7.5.1") {
        exclude(group = "jakarta.ws.rs", module = "jakarta.ws.rs-api")
    }
    implementation("io.quarkus:quarkus-kafka-streams")
    implementation("io.quarkus:quarkus-micrometer")
    implementation("io.quarkus:quarkus-micrometer-registry-prometheus")
    implementation("io.quarkus:quarkus-arc")
    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")
    testImplementation("uk.co.jemos.podam:podam:8.0.0.RELEASE")

    annotationProcessor("io.soabase.record-builder:record-builder-processor:37")
    compileOnly("io.soabase.record-builder:record-builder-core:37")

}

group = "org.acme"
version = "1.0.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}
tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
