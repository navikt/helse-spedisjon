package no.nav.helse.spedisjon

import no.nav.helse.spedisjon.AktørregisteretClient.CachedAktørregisteretClient
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

internal class CachedAktørregisteretClientTest {
    private companion object {
        private const val AKTØR1 = "aktør1"
        private const val AKTØR2 = "aktør2"
        private const val AKTØR3 = "aktør3"
        private const val FØDSELSNUMMER1 = "fnr1"
        private const val FØDSELSNUMMER2 = "fnr2"
    }

    private lateinit var testClient: TestClient
    private lateinit var client: AktørregisteretClient

    @BeforeEach
    fun setup() {
        testClient = TestClient(mapOf(
            AKTØR1 to FØDSELSNUMMER1,
            AKTØR2 to FØDSELSNUMMER2
        ))
        client = CachedAktørregisteretClient(testClient)
    }

    @Test
    fun `henter fra cache når verdi finnes`() {
        assertEquals(FØDSELSNUMMER1, client.hentFødselsnummer(AKTØR1))
        assertEquals(FØDSELSNUMMER1, client.hentFødselsnummer(AKTØR1))
        assertEquals(FØDSELSNUMMER2, client.hentFødselsnummer(AKTØR2))
        assertEquals(FØDSELSNUMMER2, client.hentFødselsnummer(AKTØR2))
        assertNull(client.hentFødselsnummer(AKTØR3))
        assertNull(client.hentFødselsnummer(AKTØR3))
        assertEquals(1, testClient.oppslagteller(AKTØR1))
        assertEquals(1, testClient.oppslagteller(AKTØR2))
        assertEquals(2, testClient.oppslagteller(AKTØR3))
    }

    @Test
    fun `henter fra cache når verdi er expired`() {
        client = CachedAktørregisteretClient(testClient, Duration.ofMillis(0))
        assertEquals(FØDSELSNUMMER1, client.hentFødselsnummer(AKTØR1))
        assertEquals(FØDSELSNUMMER1, client.hentFødselsnummer(AKTØR1))
        assertEquals(2, testClient.oppslagteller(AKTØR1))
    }

    private class TestClient(private val responses: Map<String, String>) : AktørregisteretClient {
        private val oppslagteller = mutableMapOf<String, Int>()

        internal fun oppslagteller(aktørId: String) = oppslagteller[aktørId] ?: 0
        override fun hentFødselsnummer(aktørId: String) = responses[aktørId].also {
            oppslagteller[aktørId] = oppslagteller.getOrDefault(aktørId, 0) + 1
        }
    }
}
