import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension

val junitJupiterVersion = "5.11.3"
val rapidsAndRiversVersion = "2024111021211731270108.8f718ca4927d"
val tbdLibsVersion = "2024.11.10-21.15-45a429b5"

plugins {
    base
    kotlin("jvm") version "2.0.21" apply false
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    ext.set("rapidsAndRiversVersion", rapidsAndRiversVersion)
    ext.set("tbdLibsVersion", tbdLibsVersion)

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

    configure<KotlinJvmProjectExtension> {
        jvmToolchain {
            languageVersion.set(JavaLanguageVersion.of("21"))
        }
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