package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

internal class SendteFrilansSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser sendte søknader`() {
        testRapid.sendTestMessage(SØKNAD)
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("sendt_søknad_frilans")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", false) })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor sendTilGosys=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("sendTilGosys") })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `ignorer sendte søknader hvor sendTilGosys=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("sendTilGosys", true) })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=false`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", false) })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=null`() {
        testRapid.sendTestMessage(SØKNAD.json { it.putNull("utenlandskSykmelding") })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }

    @Test
    fun `leser sendte søknader hvor utenlandskSykmelding=true`() {
        testRapid.sendTestMessage(SØKNAD.json { it.put("utenlandskSykmelding", true) })
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("sendt_søknad_frilans")
    }


    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient)
        SendteFrilansSøknader(
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
            "arbeidsgiver": null,
            "opprettet": "${LocalDateTime.now()}",
            "sendtNav": "$OPPRETTET_DATO",
            "soknadsperioder": [],
            "fravar": null,
            "status": "SENDT",
            "type": "SELVSTENDIGE_OG_FRILANSERE",
            "arbeidssituasjon": "FRILANSER",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }"""
    }
}
