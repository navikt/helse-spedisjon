package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.mockk
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class SendteSøknaderArbeidsgiverTest : AbstractRiverTest() {

    @Test
    fun `leser sendte søknader`() {
        testRapid.sendTestMessage(SØKNAD)
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", false) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("sendTilGosys") })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `ignorer sendte søknader hvor sendTilGosys=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", true) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", false) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("utenlandskSykmelding") })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", true) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_arbeidsgiver")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient, mockk(relaxed = true))
        SendteSøknaderArbeidsgiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    private companion object {
        @Language("JSON")
        private val SØKNAD = """
        {
            "id": "${UUID.randomUUID()}",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "sendtArbeidsgiver": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": [],
            "status": "SENDT",
            "type": "ARBEIDSTAKERE",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }
}
