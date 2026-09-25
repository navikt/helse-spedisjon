package no.nav.helse.spedisjon.api

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.naisful.NaisEndpoints
import com.github.navikt.tbd_libs.naisful.standardApiModule
import com.github.navikt.tbd_libs.naisful.test.TestContext
import com.github.navikt.tbd_libs.naisful.test.plainTestApp
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.ContentType.Application.Json
import io.ktor.server.plugins.*
import io.ktor.server.routing.*
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.mockk.every
import io.mockk.mockk
import java.io.IOException
import java.util.*
import no.nav.helse.spedisjon.api.tjeneste.Meldingtjeneste
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

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
    fun `kanalfeil gir midlertidig feil`() = e2e(meldingstjeneste) {
        every {
            meldingstjeneste.nyMelding(any())
        } throws BadRequestException("Failed to convert request body", IOException("connection closed"))

        client.post("/api/melding") {
            contentType(Json)
            setBody(mapOf(
                "type" to "ny_søknad",
                "fnr" to "fnr",
                "eksternDokumentId" to UUID.randomUUID(),
                "duplikatkontroll" to "unik_nøkkel",
                "jsonBody" to "{}"
            ))
        }.also { response ->
            assertEquals(HttpStatusCode.ServiceUnavailable, response.status)
            assertEquals("urn:error:temporary", response.bodyAsText().let {
                jacksonObjectMapper().readTree(it).get("type").asText()
            })
        }
    }

    @Test
    fun `ugyldig json gir bad request`() = e2e(meldingstjeneste) {
        client.post("/api/melding") {
            contentType(Json)
            setBody("{")
        }.also { response ->
            assertEquals(HttpStatusCode.BadRequest, response.status)
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
        val objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
        plainTestApp(
            testApplicationModule = {
                standardApiModule(
                    meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
                    objectMapper = objectMapper,
                    callLogger = LoggerFactory.getLogger("ApiTest"),
                    naisEndpoints = NaisEndpoints.Default,
                    callIdHeaderName = "callId",
                    preStopHook = {},
                    statusPagesConfig = { spedisjonStatusPages() }
                )
                routing {
                    api(meldingtjeneste)
                }
            },
            testClientObjectMapper = objectMapper,
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
        val duplikatkontroll: String,
        val jsonBody: String
    )
}
