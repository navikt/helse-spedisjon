package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.AzureToken
import com.github.navikt.tbd_libs.azure.AzureTokenProvider
import com.github.navikt.tbd_libs.result_object.Result
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

internal class HttpMeldingtjenesteTest {
    private val httpClient = mockk<HttpClient>()
    private val tokenProvider = mockk<AzureTokenProvider> {
        every { bearerToken(any()) } returns Result.Ok(
            AzureToken("token", LocalDateTime.now().plusHours(1))
        )
    }
    private val meldingtjeneste = HttpMeldingtjeneste(
        httpClient = httpClient,
        tokenProvider = tokenProvider,
        objectMapper = jacksonObjectMapper(),
        baseUrl = "http://spedisjon",
        scope = "scope",
        retryUtsettelser = { listOf(Duration.ZERO).iterator() }
    )
    private val request = NyMeldingRequest(
        type = "ny_søknad",
        fnr = "fnr",
        eksternDokumentId = UUID.randomUUID(),
        duplikatkontroll = "duplikatkontroll",
        jsonBody = "{}"
    )

    @Test
    fun `kjører retry ved midlertidig feil`() {
        val internDokumentId = UUID.randomUUID()
        every {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        } returnsMany listOf(
            response(503, """{"type":"urn:error:temporary","title":"Service Unavailable","status":503,"detail":"Spedisjon-API er utilgjengelig: Channel was cancelled"}"""),
            response(200, """{"internDokumentId":"$internDokumentId"}""")
        )

        assertEquals(internDokumentId, meldingtjeneste.nyMelding(request).internDokumentId)
        verify(exactly = 2) {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        }
    }

    @Test
    fun `kjører ikke retry ved bad request`() {
        every {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        } returns response(
            400,
            """{"type":"urn:error:bad_request","title":"Bad Request","status":400,"detail":"Ugyldig request"}"""
        )

        assertThrows(RuntimeException::class.java) {
            meldingtjeneste.nyMelding(request)
        }
        verify(exactly = 1) {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        }
    }

    @Test
    fun `kjører ikke retry ved 500`() {
        every {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        } returns response(
            500,
            """{"type":"urn:error:internal_error","title":"Internal Server Error","status":500,"detail":"Uventet feil"}"""
        )

        assertThrows(RuntimeException::class.java) {
            meldingtjeneste.nyMelding(request)
        }
        verify(exactly = 1) {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        }
    }

    @Test
    fun `kjører retry ved 502, 503 og 504`() {
        val internDokumentId = UUID.randomUUID()
        listOf(502, 503, 504).forEach { status ->
            io.mockk.clearMocks(httpClient, answers = false)
            every {
                httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
            } returnsMany listOf(
                response(status, """{"type":"urn:error:temporary","title":"midlertidig","status":$status,"detail":"midlertidig feil"}"""),
                response(200, """{"internDokumentId":"$internDokumentId"}""")
            )

            assertEquals(internDokumentId, meldingtjeneste.nyMelding(request).internDokumentId)
            verify(exactly = 2) {
                httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
            }
        }
    }

    @Test
    fun `kaster exception når alle retry-forsøk feiler`() {
        val meldingtjenesteMedFlereForsøk = HttpMeldingtjeneste(
            httpClient = httpClient,
            tokenProvider = tokenProvider,
            objectMapper = jacksonObjectMapper(),
            baseUrl = "http://spedisjon",
            scope = "scope",
            retryUtsettelser = { listOf(Duration.ZERO, Duration.ZERO).iterator() }
        )
        every {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        } returns response(
            503,
            """{"type":"urn:error:temporary","title":"Service Unavailable","status":503,"detail":"Spedisjon-API er utilgjengelig: Channel was cancelled"}"""
        )

        assertThrows(RuntimeException::class.java) {
            meldingtjenesteMedFlereForsøk.nyMelding(request)
        }
        // 2 utsettelser => 2 forsøk i løkka + 1 siste forsøk utenfor løkka = 3 kall
        verify(exactly = 3) {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        }
    }

    @Test
    fun `behandler duplikat (409) uten retry`() {
        val internDokumentId = UUID.randomUUID()
        every {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        } returns response(409, """{"internDokumentId":"$internDokumentId"}""")

        assertEquals(internDokumentId, meldingtjeneste.nyMelding(request).internDokumentId)
        verify(exactly = 1) {
            httpClient.send(any(), any<HttpResponse.BodyHandler<String>>())
        }
    }

    private fun response(status: Int, body: String) =
        mockk<HttpResponse<String>> {
            every { statusCode() } returns status
            every { body() } returns body
        }
}
