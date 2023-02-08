package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import no.nav.helse.rapids_rivers.RapidsConnection
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class SendteSøknaderNavTest : AbstractRiverTest() {

    @Test
    fun `leser sendte søknader`() {
        testRapid.sendTestMessage(SØKNAD)
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "sendt_søknad_nav")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", false) })
        sendBerikelse()
        assertEquals(1, antallMeldinger())
        assertSendteEvents("behov", "sendt_søknad_nav")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("sendTilGosys") })
        sendBerikelse()
        assertEquals(1, antallMeldinger())
        assertSendteEvents("behov", "sendt_søknad_nav")
    }

    @Test
    fun `ignorer sendte søknader hvor sendTilGosys=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", true) })
        assertEquals(0, antallMeldinger())
        assertSendteEvents()
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", false) })
        sendBerikelse()
        assertEquals(1, antallMeldinger())
        assertSendteEvents("behov", "sendt_søknad_nav")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("utenlandskSykmelding") })
        sendBerikelse()
        assertEquals(1, antallMeldinger())
        assertSendteEvents("behov", "sendt_søknad_nav")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", true) })
        sendBerikelse()
        assertEquals(1, antallMeldinger())
        assertSendteEvents("behov", "sendt_søknad_nav")
    }


    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        val personBerikerMediator = PersonBerikerMediator(MeldingDao(dataSource), BerikelseDao(dataSource), meldingMediator)
        SendteSøknaderNav(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        PersoninfoBeriker(rapidsConnection, personBerikerMediator)
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
            "sendtNav": "${LocalDateTime.now()}",
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
