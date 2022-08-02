package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class SendteSøknaderArbeidsgiverTest : AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    fun `leser sendte søknader`() {
        testRapid.sendTestMessage(SØKNAD)
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver", "behov")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", false) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver", "behov")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("sendTilGosys") })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver", "behov")
    }

    @Test
    fun `ignorer sendte søknader hvor sendTilGosys=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", true) })
        assertEquals(0, antallMeldinger())
        assertSendteEvents()
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        SendteSøknaderArbeidsgiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource), aktørregisteretClient)
        )
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }

    private companion object {
        @Language("JSON")
        private val SØKNAD = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "sendtArbeidsgiver": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "egenmeldinger": [],
            "fravar": [],
            "status": "SENDT",
            "type": "ARBEIDSTAKERE",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }
}
