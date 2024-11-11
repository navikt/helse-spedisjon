package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import org.junit.jupiter.api.Assertions.*
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
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": "2020-01-01",
    "matcherSpleis": true
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        manipulerTimeoutOgPubliser()
        assertSendteEvents("inntektsmelding")
    }

    @Test
    fun `ignorerer inntektsmeldinger uten fnr`() {
        testRapid.sendTestMessage("""
{
    "inntektsmeldingId": "id",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": "2020-01-01",
    "matcherSpleis": true
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
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": null, 
    "matcherSpleis": true
}"""
        )
        manipulerTimeoutOgPubliser()
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger forskjellig duplikatkontroll`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse") )
        manipulerTimeoutOgPubliser()
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2") )
        assertEquals(2, antallMeldinger(FØDSELSNUMMER))
        manipulerTimeoutOgPubliser()
        assertSendteEvents("inntektsmelding", "inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger - begge er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse", "noe") )
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2", "noeAnnet") )
        manipulerTimeoutOgPubliser()
        assertSendteEvents("inntektsmelding", "inntektsmelding")
        inntektsmeldinger() .forEach {
            assertTrue(it.path("harFlereInntektsmeldinger").asBoolean())
        }
    }

    @Test
    fun `flere inntektsmeldinger - en er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse", "noe") )
        testRapid.sendTestMessage( inntektsmelding("id2", "virksomhetsnummer", "arkivreferanse2", "noe_annet") )
        manipulerTimeoutOgPubliser()
        assertSendteEvents("inntektsmelding", "inntektsmelding")
        assertEquals(2, inntektsmeldinger().size)
        assertEquals("id", inntektsmeldinger().first().get("inntektsmeldingId").asText())
        assertTrue(inntektsmeldinger().first().path("harFlereInntektsmeldinger").asBoolean())
        assertTrue(inntektsmeldinger().last().path("harFlereInntektsmeldinger").asBoolean())
    }

    @Test
    fun `leser ikke inn inntektsmeldinger hvis matcherSpleis er false`() {
        testRapid.sendTestMessage( inntektsmelding("id", "virksomhetsnummer", "arkivreferanse", "noe", matcherSpleis = false) )
        assertEquals(0, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }


    private fun inntektsmeldinger() : List<JsonNode> {
        return (0 until testRapid.inspektør.size).mapNotNull {
            val message = testRapid.inspektør.message(it)
            if (message.path("@event_name").asText() == "inntektsmelding") message
            else null
        }
    }

    private fun inntektsmelding(id: String, virksomhetsnummer: String, arkivreferanse: String, arbeidsforholdId: String? = null, matcherSpleis: Boolean = true) : String {
        val arbeidsforholdIdJson = if (arbeidsforholdId == null) "" else """ "arbeidsforholdId": "$arbeidsforholdId", """
        return """
{
    "inntektsmeldingId": "$id",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    $arbeidsforholdIdJson
    "virksomhetsnummer": "$virksomhetsnummer",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "${LocalDateTime.now()}",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "$arkivreferanse",
    "foersteFravaersdag": null,
    "matcherSpleis": $matcherSpleis
}"""
    }

    private lateinit var inntektsmeldingMediator: InntektsmeldingMediator
    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {
        val speedClient = mockSpeed()
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), speedClient)
        inntektsmeldingMediator = InntektsmeldingMediator(dataSource, speedClient, meldingMediator = meldingMediator)
        LogWrapper(testRapid, meldingMediator).apply {
            Inntektsmeldinger(this, inntektsmeldingMediator)
        }
    }

    private fun manipulerTimeoutOgPubliser() {
        manipulerTimeoutInntektsmelding(FØDSELSNUMMER)
        inntektsmeldingMediator.ekspeder(testRapid)
    }
}
