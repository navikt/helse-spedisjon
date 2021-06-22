package no.nav.helse.spedisjon

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import com.github.tomakehurst.wiremock.matching.AnythingPattern
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.TestInstance.Lifecycle

@TestInstance(Lifecycle.PER_CLASS)
internal class AktørregisteretRestClientTest {

    private companion object {
        private val server: WireMockServer = WireMockServer(WireMockConfiguration.options().dynamicPort())

        private const val TOKEN = "bearer token"
        private const val AKTØR = "123456"
        private const val FØDSELSNUMMER = "12345678911"
    }

    private val stsRestClientMock = mockk<StsRestClient>()
    private lateinit var aktørregisterClient: AktørregisteretClient

    init {
        every {
            stsRestClientMock.token()
        } returns TOKEN
    }

    @BeforeAll
    fun start() {
        server.start()
    }

    @AfterAll
    fun stop() {
        server.stop()
    }

    @BeforeEach
    fun configure() {
        configureFor(server.port())
        aktørregisterClient = AktørregisteretClient.AktørregisteretRestClient(
            baseUrl = server.baseUrl(),
            stsRestClient = stsRestClientMock
        )
    }

    @Test
    fun `henter fødselsnummer`() {
        stub(AKTØR, okResponse(AKTØR))
        assertEquals(FØDSELSNUMMER, aktørregisterClient.hentFødselsnummer(AKTØR))
    }

    @Test
    fun `henter aktørId`() {
        stub(FØDSELSNUMMER, okResponse(FØDSELSNUMMER))
        assertEquals(AKTØR, aktørregisterClient.hentAktørId(FØDSELSNUMMER))
    }

    @Test
    fun `gir null ved tomt resultat`() {
        stub(AKTØR, emptyResponse(AKTØR))
        assertNull(aktørregisterClient.hentFødselsnummer(AKTØR))
    }

    @Test
    fun `gir null ved feil`() {
        stub(AKTØR, errorResponse(AKTØR))
        assertNull(aktørregisterClient.hentFødselsnummer(AKTØR))
    }

    @Test
    fun `gir null ved ugyldig json`() {
        stub(AKTØR, "this is not json")
        assertNull(aktørregisterClient.hentFødselsnummer(AKTØR))
    }

    private val aktørregisterRequestMapping = get(urlPathEqualTo("/api/v1/identer"))
        .withQueryParam("gjeldende", equalTo("true"))
        .withHeader("Authorization", equalTo("Bearer $TOKEN"))
        .withHeader("Nav-Call-Id", AnythingPattern())
        .withHeader("Nav-Consumer-Id", equalTo("spedisjon"))
        .withHeader("Accept", equalTo("application/json"))

    private fun stub(ident: String, response: String) {
        stubFor(
            aktørregisterRequestMapping
                .withHeader("Nav-Personidenter", equalTo(ident))
                .willReturn(ok(response))
        )
    }

    private fun emptyResponse(ident: String) = """
{
  "$ident": {
    "identer": [],
    "feilmelding": null
  }
}
"""

    private fun okResponse(ident: String) = """
{
  "$ident": {
    "identer": [
      {
        "ident": "$AKTØR",
        "identgruppe": "AktoerId",
        "gjeldende": true
      },
      {
        "ident": "$FØDSELSNUMMER",
        "identgruppe": "NorskIdent",
        "gjeldende": true
      }
    ],
    "feilmelding": null
  }
}
"""

    private fun errorResponse(ident: String) = """
{
    "$ident": {
        "identer": null,
        "feilmelding": "Den angitte personidenten finnes ikke"
    }
}
"""
}
