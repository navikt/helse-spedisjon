package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*

internal class FremtidigSøknaderTest: AbstractRiverTest() {

    @Test
    fun `tar inn fremtidig søknad`() {
        testRapid.sendTestMessage(søknad())
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
        assertEquals("NY", testRapid.inspektør.field(0, "status").textValue())
        assertEquals(true, testRapid.inspektør.field(0, "fremtidig_søknad").booleanValue())
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `ignorerer ny søknad om vi har en fremtidig`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        testRapid.sendTestMessage(søknad("NY"))
        Assertions.assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient)
        FremtidigSøknaderRiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    private fun søknad(status: String = "FREMTIDIG", type: String = "ARBEIDSTAKERE") = """
        {
            "id": "afbb6489-f3f5-4b7d-8689-af1d7b53087a",
            "fnr": "$FØDSELSNUMMER",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "$OPPRETTET_DATO",
            "type": "$type",
            "soknadsperioder": [],
            "status": "$status",
            "sykmeldingId": "${UUID.randomUUID()}",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }
    """
}
