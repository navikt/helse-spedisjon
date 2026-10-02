plugins {
    id("no.nav.sykepenger.deployable")
}

sykepengerDeployable {
    mainClass = "no.nav.helse.opprydding.AppKt"
    imageName = "${rootProject.name}-opprydding-dev"
}

dependencies {
    api(libs.rapids.and.rivers)
    implementation(libs.sykepengerLibs.logging)
    implementation(libs.cloud.sql.postgres.socket.factory)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)
    implementation(libs.hikaricp)

    testImplementation(libs.tbd.libs.postgres.testdatabaser)
    testImplementation(libs.tbd.libs.rapids.and.rivers.test)
}
