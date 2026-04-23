package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import java.util.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class NyeSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser nye søknader`() {
        testRapid.sendTestMessage("""
{
    "id": "${UUID.randomUUID()}",
    "fnr": "$FØDSELSNUMMER",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "2020-01-02T02:00:00",
    "soknadsperioder": [],
    "status": "NY",
    "type": "ARBEIDSTAKERE",
    "sykmeldingId": "${UUID.randomUUID()}",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}""")
        Assertions.assertEquals(1, antallMeldinger())
        assertSendteEvents("ny_søknad")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        val speedClient = mockSpeed()
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }
}
