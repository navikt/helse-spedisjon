package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class SendteFrilansSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser sendte søknader`() {
        testRapid.sendTestMessage(SØKNAD)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", false) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("sendTilGosys") })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `ignorer sendte søknader hvor sendTilGosys=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", true) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", false) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("utenlandskSykmelding") })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", true) })
        assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }


    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient)
        SendteFrilansSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    private companion object {
        @Language("JSON")
        private val SØKNAD = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": null,
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "fravar": null,
            "status": "SENDT",
            "type": "SELVSTENDIGE_OG_FRILANSERE",
            "arbeidssituasjon": "FRILANSER",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }
}
