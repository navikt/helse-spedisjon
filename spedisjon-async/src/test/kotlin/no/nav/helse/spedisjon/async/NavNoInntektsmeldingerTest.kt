package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearMocks
import io.mockk.mockk
import java.time.LocalDateTime
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class NavNoInntektsmeldingerTest : AbstractRiverTest() {

    @Test
    fun `leser inntektsmeldinger`() {
        testRapid.sendTestMessage("""
{
    "avsenderSystem": {"navn": "NAV_NO" },
    "inntektsmeldingId": "ac85bd20-7a1e-45e0-afaf-d946db30acd1",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "$OPPRETTET_DATO",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "opphoerAvNaturalytelser": [],
    "matcherSpleis": true
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `ignorerer inntektsmeldinger med feil avsendersystem`() {
        testRapid.sendTestMessage("""
{
    "avsenderSystem": { "navn": "LPS" },
    "inntektsmeldingId": "ac85bd20-7a1e-45e0-afaf-d946db30acd1",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "$OPPRETTET_DATO",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "opphoerAvNaturalytelser": [],
    "matcherSpleis": true
}"""
        )
        assertEquals(0, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `flere inntektsmeldinger forskjellig duplikatkontroll`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse") )
        testRapid.sendTestMessage( inntektsmelding("66072deb-8586-4fa3-b41a-2e21850fd7db", "virksomhetsnummer", "arkivreferanse2") )
        assertEquals(2, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding", "inntektsmelding")
    }

    @Test
    fun `leser ikke inn inntektsmeldinger hvis matcherSpleis er false`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse", "noe", matcherSpleis = false) )
        assertEquals(0, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    private fun inntektsmelding(id: String, virksomhetsnummer: String, arkivreferanse: String, arbeidsforholdId: String? = null, matcherSpleis: Boolean = true) : String {
        val arbeidsforholdIdJson = if (arbeidsforholdId == null) "" else """ "arbeidsforholdId": "$arbeidsforholdId", """
        return """
{
    "avsenderSystem": {"navn": "NAV_NO" },
    "inntektsmeldingId": "$id",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    $arbeidsforholdIdJson
    "virksomhetsnummer": "$virksomhetsnummer",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "$arkivreferanse",
    "opphoerAvNaturalytelser": [],
    "matcherSpleis": $matcherSpleis
}"""
    }

    private val dokumentProducerMock = mockk<KafkaProducer<String, String>>(relaxed = true)
    override fun createRiver(rapidsConnection: RapidsConnection, meldingtjeneste: Meldingtjeneste) {
        clearMocks(dokumentProducerMock)
        val ekspederingMediator = EkspederingMediator(
            dao = EkspederingDao(::dataSource),
            rapidsConnection = rapidsConnection,
        )
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(meldingtjeneste, speedClient, ekspederingMediator)
        LogWrapper(testRapid, meldingMediator).apply {
            NavNoInntektsmeldinger(this, meldingMediator)
        }
    }
}
