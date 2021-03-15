package no.nav.helse.spedisjon

import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class FremtidigSøknaderTest: AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    fun `tar inn fremtidig søknad`() {
        testRapid.sendTestMessage(fremtidigSøknad())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    @Test
    fun `ignorerer ny søknad om vi har en fremtidig`() {
        assertTrue(false)
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        FremtidigSøknaderRiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient, true)
        )
    }

    fun fremtidigSøknad() = """
        {
            "id": "id",
            "fnr": "$FØDSELSNUMMER",
            "aktorId": "$AKTØR",
            "arbeidsgiver": {
                "orgnummer": "1234"
            },
            "opprettet": "${LocalDateTime.now()}",
            "soknadsperioder": [],
            "status": "FREMTIDIG",
            "sykmeldingId": "id",
            "fom": "2020-01-01",
            "tom": "2020-01-01"

    """.trimIndent()
}