package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearMocks
import io.mockk.mockk
import org.apache.kafka.clients.producer.KafkaProducer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

internal class LpsOgAltinnInntektsmeldingerTest : AbstractRiverTest() {

    @Test
    fun `ingen sendeklare inntektsmeldinger`() {
        assertEquals(0, testRapid.inspektør.size)
    }

    @Test
    fun `leser inntektsmeldinger`() {
        testRapid.sendTestMessage("""
{
    "inntektsmeldingId": "ac85bd20-7a1e-45e0-afaf-d946db30acd1",
    "arbeidstakerFnr": "$FØDSELSNUMMER",
    "virksomhetsnummer": "1234",
    "arbeidsgivertype": "BEDRIFT",
    "beregnetInntekt": "1000",
    "mottattDato": "$OPPRETTET_DATO",
    "endringIRefusjoner": [],
    "arbeidsgiverperioder": [],
    "ferieperioder": [],
    "status": "GYLDIG",
    "arkivreferanse": "arkivref",
    "foersteFravaersdag": "2020-01-01",
    "format": "Inntektsmelding"
}""")
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
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
    "format": "Inntektsmelding"
}"""
        )
        assertEquals(0, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents()
    }

    @Test
    fun `leser inntektsmeldinger uten første fraværsdag`() {
        testRapid.sendTestMessage("""
{
    "inntektsmeldingId": "ac85bd20-7a1e-45e0-afaf-d946db30acd1",
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
    "format": "Inntektsmelding"
}"""
        )
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding")
    }

    @Test
    fun `publiserer samme id`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse") )
        val id = testRapid.inspektør.field(0, "@id").asText()
        val inntetsmeldingFrånDatabasen = inntektsmeldingFrånDatabasen()
        assertEquals(id, inntetsmeldingFrånDatabasen.first.toString())
        assertFalse(inntetsmeldingFrånDatabasen.second.hasNonNull("@id"))
    }

    private fun inntektsmeldingFrånDatabasen() : Pair<UUID, JsonNode> {
        return meldingstjeneste.meldinger.single().let { dto ->
            dto.internDokumentId to objectMapper.readTree(dto.jsonBody)
        }
    }

    @Test
    fun `flere inntektsmeldinger forskjellig duplikatkontroll`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse") )
        testRapid.sendTestMessage( inntektsmelding("66072deb-8586-4fa3-b41a-2e21850fd7db", "virksomhetsnummer", "arkivreferanse2") )
        assertEquals(2, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("inntektsmelding", "inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger - begge er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse", "noe") )
        testRapid.sendTestMessage( inntektsmelding("66072deb-8586-4fa3-b41a-2e21850fd7db", "virksomhetsnummer", "arkivreferanse2", "noeAnnet") )
        assertSendteEvents("inntektsmelding", "inntektsmelding")
    }

    @Test
    fun `flere inntektsmeldinger - en er beriket`() {
        testRapid.sendTestMessage( inntektsmelding("afbb6489-f3f5-4b7d-8689-af1d7b53087a", "virksomhetsnummer", "arkivreferanse", "noe") )
        testRapid.sendTestMessage( inntektsmelding("66072deb-8586-4fa3-b41a-2e21850fd7db", "virksomhetsnummer", "arkivreferanse2", "noe_annet") )
        assertSendteEvents("inntektsmelding", "inntektsmelding")
        assertEquals(2, inntektsmeldinger().size)
        assertEquals("afbb6489-f3f5-4b7d-8689-af1d7b53087a", inntektsmeldinger().first().get("inntektsmeldingId").asText())
    }

    private fun inntektsmeldinger() : List<JsonNode> {
        return (0 until testRapid.inspektør.size).mapNotNull {
            val message = testRapid.inspektør.message(it)
            if (message.path("@event_name").asText() == "inntektsmelding") message
            else null
        }
    }

    private fun inntektsmelding(id: String, virksomhetsnummer: String, arkivreferanse: String, arbeidsforholdId: String? = null) : String {
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
    "format": "Inntektsmelding"
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
            LpsOgAltinnInntektsmeldinger(this, meldingMediator)
        }
    }

    private companion object {
        val objectMapper = jacksonObjectMapper()
    }
}
