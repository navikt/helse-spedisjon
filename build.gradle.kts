val junitJupiterVersion = "5.10.2"
val rapidsAndRiversVersion = "2024020507581707116327.1c34df474331"
val tbdLibsVersion = "2024.05.31-08.02-2c3441c1"

plugins {
    base
    kotlin("jvm") version "1.9.22" apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    ext.set("rapidsAndRiversVersion", rapidsAndRiversVersion)

    repositories {
        val githubPassword: String? by project
        mavenCentral()
        /* ihht. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
            så plasseres github-maven-repo (med autentisering) før nav-mirror slik at github actions kan anvende førstnevnte.
            Det er fordi nav-mirroret kjører i Google Cloud og da ville man ellers fått unødvendige utgifter til datatrafikk mellom Google Cloud og GitHub
         */
        maven {
            url = uri("https://maven.pkg.github.com/navikt/maven-release")
            credentials {
                username = "x-access-token"
                password = githubPassword
            }
        }
        maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
    }

    val testImplementation by configurations
    val testRuntimeOnly by configurations
    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
        testImplementation("com.github.navikt.tbd-libs:postgres-testdatabaser:$tbdLibsVersion")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
        withSourcesJar()
    }

    tasks {
        withType<Test> {
            useJUnitPlatform()
            testLogging {
                events("skipped", "failed")
            }
        }
    }
}