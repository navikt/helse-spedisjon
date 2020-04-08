package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingerTest : AbstractRiverTest() {

    private val aktørregisteretClient = mockk<AktørregisteretClient>()

    @Test
    internal fun `leser inntektsmeldinger`() {
        testRapid.sendTestMessage("""
{
    "inntektsmeldingId": "id",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    "arbeidstakerAktorId": "$AKTØR",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": "2020-01-01"
}""")

        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    @Test
    internal fun `leser inntektsmeldinger uten fnr`() {
        every {
            aktørregisteretClient.hentFødselsnummer(AKTØR)
        } returns FØDSELSNUMMER

        testRapid.sendTestMessage("""
{
    "inntektsmeldingId": "id",
    "arbeidstakerAktorId": "$AKTØR",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": "2020-01-01"
}"""
        )

        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        Inntektsmeldinger(
            rapidsConnection = rapidsConnection,
            meldingMediator = MeldingMediator(MeldingDao(dataSource), aktørregisteretClient, true)
        )
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }
}
