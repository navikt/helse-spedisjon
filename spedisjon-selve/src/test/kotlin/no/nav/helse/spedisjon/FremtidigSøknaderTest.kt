package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID
import javax.sql.DataSource

internal class FremtidigSøknaderTest: AbstractRiverTest() {

    @Test
    fun `tar inn fremtidig søknad`() {
        testRapid.sendTestMessage(søknad())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
        assertEquals("NY", testRapid.inspektør.message(0)["status"].textValue())
        assertEquals(true, testRapid.inspektør.message(0)["fremtidig_søknad"].booleanValue())
    }

    @Test
    fun `ignorerer ny søknad om vi har en fremtidig`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        testRapid.sendTestMessage(søknad("NY"))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient, mockk(relaxed = true))
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
