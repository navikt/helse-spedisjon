package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class NyeSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser nye søknader`() {
        testRapid.sendTestMessage("""
{
    "id": "id",
    "fnr": "$FØDSELSNUMMER",
    "aktorId": "$AKTØR",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "${LocalDateTime.now()}",
    "soknadsperioder": [],
    "status": "NY",
    "type": "ARBEIDSTAKERE",
    "sykmeldingId": "id",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient)
        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }
}
