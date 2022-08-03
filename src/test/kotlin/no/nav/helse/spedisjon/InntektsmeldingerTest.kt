package no.nav.helse.spedisjon

import io.mockk.clearAllMocks
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingerTest : AbstractRiverTest() {

    @Test
    fun `leser inntektsmeldinger`() {
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
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "inntektsmelding")
    }

    @Test
    fun `ignorerer inntektsmeldinger uten fnr`() {
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
        assertEquals(0, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `leser inntektsmeldinger uten første fraværsdag`() {
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
    "foersteFravaersdag": null
}"""
        )
        sendBerikelse()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "inntektsmelding")
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        LogWrapper(testRapid, meldingMediator = meldingMediator).apply {
            Inntektsmeldinger(
                rapidsConnection = this,
                meldingMediator = meldingMediator
            )
        }
        PersoninfoBeriker(testRapid, meldingMediator)
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }
}
