package no.nav.helse.spedisjon

import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import javax.sql.DataSource

internal class FremtidigSøknaderTest: AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    fun `tar inn fremtidig søknad`() {
        testRapid.sendTestMessage(søknad())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals("NY", testRapid.inspektør.message(0)["status"].textValue())
        assertEquals(true, testRapid.inspektør.message(0)["fremtidig_søknad"].booleanValue())
    }

    @Test
    fun `ignorerer ny søknad om vi har en fremtidig`() {
        testRapid.sendTestMessage(søknad("FREMTIDIG"))
        testRapid.sendTestMessage(søknad("NY"))
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        FremtidigSøknaderRiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient)
        )

        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient)
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

    """.trimIndent()

}
