package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class SendteSøknaderArbeidsgiverTest : AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    internal fun `leser sendte søknader`() {
        testRapid.sendTestMessage("""
{
    "id": "id",
    "fnr": "$FØDSELSNUMMER",
    "aktorId": "$AKTØR",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "${LocalDateTime.now()}",
    "sendtArbeidsgiver": "${LocalDateTime.now()}",
    "soknadsperioder": [],
    "egenmeldinger": [],
    "fravar": [],
    "status": "SENDT",
    "sykmeldingId": "id",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}""")

        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    @Test
    internal fun `leser sendte søknader uten fnr`() {
        every {
            aktørregisteretClient.hentFødselsnummer(AKTØR)
        } returns FØDSELSNUMMER

        testRapid.sendTestMessage("""
{
    "id": "id",
    "aktorId": "$AKTØR",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "${LocalDateTime.now()}",
    "sendtArbeidsgiver": "${LocalDateTime.now()}",
    "soknadsperioder": [],
    "egenmeldinger": [],
    "fravar": [],
    "status": "SENDT",
    "sykmeldingId": "id",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}"""
        )

        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        SendteSøknaderArbeidsgiver(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient, true),
            problemsCollector = object : ProblemsCollector {
                override fun add(type: String, problems: MessageProblems) {}
            }
        )
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }
}
