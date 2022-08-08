package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import javax.sql.DataSource

internal class FremtidigSøknaderTest: AbstractRiverTest() {

    @Test
    fun `tar inn fremtidig søknad`() {
        testRapid.sendTestMessage(søknad())
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "ny_søknad")
        assertEquals("NY", testRapid.inspektør.message(1)["status"].textValue())
        assertEquals(true, testRapid.inspektør.message(1)["fremtidig_søknad"].booleanValue())
    }

    @Test
    fun `ignorerer ny søknad om vi har en fremtidig`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        testRapid.sendTestMessage(søknad("NY"))
        sendBerikelse()
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "ny_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        FremtidigSøknaderRiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
        PersoninfoBeriker(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }

    private fun søknad(status: String = "FREMTIDIG", type: String = "ARBEIDSTAKERE") = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "$OPPRETTET_DATO",
            "type": "$type",
            "soknadsperioder": [],
            "status": "$status",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"
        }
    """
}
