val mainClass = "no.nav.helse.spedisjon.migrering.AppKt"

val logbackClassicVersion = "1.5.12"
val logbackEncoderVersion = "8.0"
val flywayVersion = "10.21.0"
val cloudSqlVersion = "1.21.0"
val postgresqlVersion = "42.7.4"
val hikariCPVersion = "6.1.0"
val kotliqueryVersion = "1.9.0"
val jacksonVersion = "2.18.1"
val kotlinxCoroutinesVersion = "1.9.0"

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$kotlinxCoroutinesVersion")

    implementation("ch.qos.logback:logback-classic:$logbackClassicVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:$logbackEncoderVersion") {
        exclude("com.fasterxml.jackson.core")
        exclude("com.fasterxml.jackson.dataformat")
    }

    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    implementation("com.google.cloud.sql:postgres-socket-factory:$cloudSqlVersion")
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.github.seratch:kotliquery:$kotliqueryVersion")
    implementation("com.zaxxer:HikariCP:$hikariCPVersion")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
}

tasks {
    val copyJars = create("copy-jars") {
        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists())
                    it.copyTo(file)
            }
        }
    }
    get("build").finalizedBy(copyJars)

    withType<Jar> {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = mainClass
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }
    }
}
