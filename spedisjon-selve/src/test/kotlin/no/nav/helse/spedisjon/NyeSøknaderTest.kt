package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*
import javax.sql.DataSource

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
    "opprettet": "$OPPRETTET_DATO",
    "soknadsperioder": [],
    "status": "NY",
    "type": "ARBEIDSTAKERE",
    "sykmeldingId": "${UUID.randomUUID()}",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient, mockk(relaxed = true))
        NyeSøknader(
            rapidsConnection = rapidsConnection,
            meldingMediator = meldingMediator
        )
    }
}
