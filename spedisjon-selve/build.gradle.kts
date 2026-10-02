plugins {
    id("no.nav.sykepenger.deployable")
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spedisjon.api.AppKt"
    imageName = "${rootProject.name}-selve"
}

dependencies {
    api(libs.tbd.libs.naisful.app)
    api(libs.tbd.libs.naisful.postgres)

    api(libs.ktor.server.auth)
    api(libs.ktor.server.auth.jwt)

    api(libs.flyway.database.postgresql)
    implementation(libs.sykepengerLibs.logging)
    implementation(libs.hikaricp)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)

    implementation(libs.jackson.datatype.jsr310)

    testImplementation(libs.tbd.libs.postgres.testdatabaser)
    testImplementation(libs.tbd.libs.naisful.test.app)
    testImplementation(libs.mockk)
}
