package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

internal class NyeSøknaderTest : AbstractRiverTest() {

    @Test
    fun `leser nye søknader`() {
        testRapid.sendTestMessage("""
{
    "id": "${UUID.randomUUID()}",
    "fnr": "$FØDSELSNUMMER",
    "aktorId": "$AKTØR",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "${LocalDateTime.now()}",
    "soknadsperioder": [],
    "status": "NY",
    "type": "ARBEIDSTAKERE",
    "sykmeldingId": "${UUID.randomUUID()}",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("ny_søknad")
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
