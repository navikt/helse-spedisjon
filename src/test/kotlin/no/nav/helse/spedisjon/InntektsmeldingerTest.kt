package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import io.mockk.clearAllMocks
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
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
        manipulerTimeoutOgPubliser()
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
        manipulerTimeoutOgPubliser()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("behov", "inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger forskjellig duplikatkontroll`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse") )
        sendBerikelse()
        manipulerTimeoutOgPubliser()
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2") )
        sendBerikelse()
        assertEquals(2, antallMeldinger(FØDSELSNUMMER))
        manipulerTimeoutOgPubliser()
        assertSendteEvents("behov", "inntektsmelding", "behov", "inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger - begge er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse") )
        sendBerikelse()
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2") )
        sendBerikelse()
        manipulerTimeoutOgPubliser()
        assertSendteEvents("behov", "behov", "inntektsmelding", "inntektsmelding")
        inntektsmeldinger() .forEach {
            assertTrue(it.path("harFlereInntektsmeldinger").asBoolean())
        }
    }

    @Test
    fun `flere inntektsmeldinger - en er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse") )
        sendBerikelse()
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2") )
        manipulerTimeoutOgPubliser()
        assertSendteEvents("behov", "behov", "inntektsmelding")
        assertEquals(1, inntektsmeldinger().size)
        assertEquals("id", inntektsmeldinger().single().get("inntektsmeldingId").asText())
        assertTrue(inntektsmeldinger().single().path("harFlereInntektsmeldinger").asBoolean())
    }


    private fun inntektsmeldinger() : List<JsonNode> {
        return (0 until testRapid.inspektør.size).mapNotNull {
            val message = testRapid.inspektør.message(it)
            if (message.path("@event_name").asText() == "inntektsmelding") message
            else null
        }
    }

    private fun inntektsmelding(id: String, virksomhetsnummer: String, arkivreferanse: String) : String{
        return """
{
    "inntektsmeldingId": "$id",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    "arbeidstakerAktorId": "$AKTØR",
    "virksomhetsnummer": "$virksomhetsnummer",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "$arkivreferanse",
    "foersteFravaersdag": null
}"""
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), BerikelseDao(dataSource))
        val inntektsmeldingMediator = InntektsmeldingMediator(dataSource)
        val personBerikerMediator = PersonBerikerMediator(MeldingDao(dataSource), BerikelseDao(dataSource), meldingMediator)
        LogWrapper(testRapid, meldingMediator).apply {
            Inntektsmeldinger(
                rapidsConnection = this,
                inntektsmeldingMediator = inntektsmeldingMediator
            )
        }
        PersoninfoBeriker(testRapid, personBerikerMediator)
    }

    @BeforeEach
    fun clear() {
        clearAllMocks()
    }

    private fun manipulerTimeoutOgPubliser(){
        manipulerTimeoutInntektsmelding(FØDSELSNUMMER)
        InntektsmeldingMediator(dataSource).republiser(testRapid)
    }
}
