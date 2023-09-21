private val testcontainersVersion = "1.18.3"
private val cloudSqlVersion = "1.11.2"
private val postgresqlVersion = "42.6.0"
private val hikariVersion = "5.0.1"
private val kotliqueryVersion = "1.9.0"
private val flywayVersion = "9.3.0"
private val junitVersion = "5.9.0"
private val rapidsAndRiversVersion = "2023050308441683096263.f5a276d7bd28"

val mainClass = "no.nav.helse.opprydding.AppKt"

plugins {
    kotlin("jvm") apply true
}

dependencies {
    api("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    implementation("com.google.cloud.sql:postgres-socket-factory:$cloudSqlVersion")
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.github.seratch:kotliquery:$kotliqueryVersion")
    implementation("com.zaxxer:HikariCP:$hikariVersion")

    testImplementation(project(":spedisjon-selve")) // for å få  tilgang på db/migrations-filene
    testImplementation("org.flywaydb:flyway-core:$flywayVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion") {
        exclude("com.fasterxml.jackson.core")
    }
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    implementation(kotlin("stdlib-jdk8"))
}

tasks {

    test {
        useJUnitPlatform()
    }

    named<Jar>("jar") {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = mainClass
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("$buildDir/libs/${it.name}")
                if (!file.exists())
                    it.copyTo(file)
            }
        }
    }
}