package no.nav.helse.spedisjon.api

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.naisful.test.TestContext
import com.github.navikt.tbd_libs.naisful.test.naisfulTestApp
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType.Application.Json
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.routing.routing
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.mockk.every
import io.mockk.mockk
import no.nav.helse.spedisjon.api.tjeneste.Meldingtjeneste
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class ApiTest {
    private val meldingstjeneste = mockk<Meldingtjeneste>()

    @BeforeEach
    fun clearMocks() {
        io.mockk.clearMocks(meldingstjeneste)
    }

    @Test
    fun `ny melding - ok`() = e2e(meldingstjeneste) {
        val internDokumentId = UUID.randomUUID()
        every {
            meldingstjeneste.nyMelding(any())
        } returns no.nav.helse.spedisjon.api.tjeneste.NyMeldingResponse(internDokumentId, true)

        client.post("/api/melding") {
            contentType(Json)
            setBody(mapOf(
                "type" to "ny_søknad",
                "fnr" to "fnr",
                "eksternDokumentId" to UUID.randomUUID(),
                "rapportertDato" to LocalDateTime.now(),
                "duplikatkontroll" to "unik_nøkkel",
                "jsonBody" to "{}"
            ))
        }.also { response ->
            assertEquals(HttpStatusCode.OK, response.status)
            val body = response.body<ForventetNyMeldingResponse>()
            assertEquals(internDokumentId, body.internDokumentId)
        }
    }

    @Test
    fun `ny melding - duplikat`() = e2e(meldingstjeneste) {
        val internDokumentId = UUID.randomUUID()
        every {
            meldingstjeneste.nyMelding(any())
        } returns no.nav.helse.spedisjon.api.tjeneste.NyMeldingResponse(internDokumentId, false)

        client.post("/api/melding") {
            contentType(Json)
            setBody(mapOf(
                "type" to "ny_søknad",
                "fnr" to "fnr",
                "eksternDokumentId" to UUID.randomUUID(),
                "rapportertDato" to LocalDateTime.now(),
                "duplikatkontroll" to "unik_nøkkel",
                "jsonBody" to "{}"
            ))
        }.also { response ->
            assertEquals(HttpStatusCode.Conflict, response.status)
            val body = response.body<ForventetNyMeldingResponse>()
            assertEquals(internDokumentId, body.internDokumentId)
        }
    }

    @Test
    fun `hent melding`() = e2e(meldingstjeneste) {
        val internDokumentId = UUID.randomUUID()
        val eksternDokumentId = UUID.randomUUID()
        every {
            meldingstjeneste.hentMeldinger(eq(listOf(internDokumentId)))
        } returns no.nav.helse.spedisjon.api.tjeneste.HentMeldingerResponse(listOf(MeldingDto(
            type = "ny_søknad",
            fnr = "fnr",
            internDokumentId = internDokumentId,
            eksternDokumentId = eksternDokumentId,
            rapportertDato = LocalDateTime.now(),
            duplikatkontroll = "unik_nøkkel",
            jsonBody = "{}"
        )))

        client.get("/api/melding/$internDokumentId").also { response ->
            assertEquals(HttpStatusCode.OK, response.status)
            val response = response.body<ForventetMeldingResponse>()
            assertEquals("ny_søknad", response.type)
            assertEquals("fnr", response.fnr)
            assertEquals(internDokumentId, response.internDokumentId)
            assertEquals(eksternDokumentId, response.eksternDokumentId)
            assertEquals("unik_nøkkel", response.duplikatkontroll)
            assertEquals("{}", response.jsonBody)
        }
    }

    @Test
    fun `hent meldinger`() = e2e(meldingstjeneste) {
        val internDokumentId = UUID.randomUUID()
        val eksternDokumentId = UUID.randomUUID()
        every {
            meldingstjeneste.hentMeldinger(eq(listOf(internDokumentId)))
        } returns no.nav.helse.spedisjon.api.tjeneste.HentMeldingerResponse(listOf(MeldingDto(
            type = "ny_søknad",
            fnr = "fnr",
            internDokumentId = internDokumentId,
            eksternDokumentId = eksternDokumentId,
            rapportertDato = LocalDateTime.now(),
            duplikatkontroll = "unik_nøkkel",
            jsonBody = "{}"
        )))

        client.get("/api/meldinger") {
            contentType(Json)
            setBody(mapOf(
                "internDokumentIder" to listOf(internDokumentId)
            ))
        }.also { response ->
            assertEquals(HttpStatusCode.OK, response.status)
            val response = response.body<ForventetHentMeldingerResponse>()
            assertEquals(1, response.meldinger.size)

            val melding = response.meldinger.single()
            assertEquals("ny_søknad", melding.type)
            assertEquals("fnr", melding.fnr)
            assertEquals(internDokumentId, melding.internDokumentId)
            assertEquals(eksternDokumentId, melding.eksternDokumentId)
            assertEquals("unik_nøkkel", melding.duplikatkontroll)
            assertEquals("{}", melding.jsonBody)
        }
    }

    private fun e2e(meldingtjeneste: Meldingtjeneste, testblokk: suspend TestContext.() -> Unit) {
        naisfulTestApp(
            testApplicationModule = {
                routing {
                    api(meldingtjeneste)
                }
            },
            objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule()),
            meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
            testblokk = testblokk
        )
    }

    data class ForventetNyMeldingResponse(val internDokumentId: UUID)

    data class ForventetHentMeldingerResponse(
        val meldinger: List<ForventetMeldingResponse>
    )

    data class ForventetMeldingResponse(
        val type: String,
        val fnr: String,
        val internDokumentId: UUID,
        val eksternDokumentId: UUID,
        val rapportertDato: LocalDateTime,
        val duplikatkontroll: String,
        val jsonBody: String
    )
}
