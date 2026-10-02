plugins {
    id("no.nav.sykepenger.deployable")
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spedisjon.async.AppKt"
    imageName = "${rootProject.name}-async"
}

dependencies {
    implementation(libs.rapids.and.rivers)
    api(libs.flyway.database.postgresql)
    implementation(libs.hikaricp)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.sykepengerLibs.logging)

    api(libs.tbd.libs.azure.token.client.default)
    api(libs.tbd.libs.retry)
    api(libs.tbd.libs.speed.client)

    testImplementation(libs.tbd.libs.postgres.testdatabaser)
    testImplementation(libs.tbd.libs.rapids.and.rivers.test)
    testImplementation(libs.mockk)
}
