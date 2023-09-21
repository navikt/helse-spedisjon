val junitJupiterVersion = "5.9.1"
val testcontainersVersion = "1.17.5"
val flywayCoreVersion = "9.7.0"
val rapidsAndRiversVersion = "2022092314391663936769.9d5d33074875"
val hikariCPVersion = "5.0.1"
val postgresqlVersion = "42.5.0"
val kotliqueryVersion = "1.9.0"
val mockkVersion = "1.13.2"
val wiremockJre8Version = "2.34.0"


repositories {
    mavenCentral()
    // Needed for com.github.navikt:rapids-and-rivers
    maven("https://jitpack.io")
}

plugins {
    kotlin("jvm") apply true
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")
    implementation("org.flywaydb:flyway-core:$flywayCoreVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.github.seratch:kotliquery:$kotliqueryVersion")

    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("com.github.tomakehurst:wiremock-jre8:$wiremockJre8Version") {
        exclude(group = "com.github.jknack.handlebars.java")
        exclude(group = "junit")
    }
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "17"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "17"
    }

    named<Jar>("jar") {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.spedisjon.AppKt"
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

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}
