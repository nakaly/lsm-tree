import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.0"
}

group = "me.st20142"
version = "1.0-SNAPSHOT"


repositories {
    mavenCentral()
}

val scalaBinary = "2.13"

dependencies {
    implementation("com.michael-bull.kotlin-result:kotlin-result:1.1.11")
    implementation(platform("com.typesafe.akka:akka-bom_${scalaBinary}:2.6.15"))

    implementation("com.typesafe.akka:akka-actor-typed_${scalaBinary}")
    testImplementation("com.typesafe.akka:akka-actor-testkit-typed_${scalaBinary}")
    testImplementation(kotlin("test-junit5"))
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.6.0")
    testImplementation("org.mockito.kotlin:mockito-kotlin:3.2.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}