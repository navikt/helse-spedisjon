package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.speed.SpeedClient
import no.nav.helse.rapids_rivers.RapidApplication
import java.net.http.HttpClient

fun main() {
    val env = System.getenv()

    val httpClient = HttpClient.newHttpClient()
    val azure = createAzureTokenClientFromEnvironment(env)
    val speedClient = SpeedClient(httpClient, jacksonObjectMapper().registerModule(JavaTimeModule()), azure)

    RapidApplication.create(env).start()
}