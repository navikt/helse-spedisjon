package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.mockk.clearMocks
import io.mockk.mockk
import java.time.LocalDateTime
import java.util.UUID
import org.apache.kafka.clients.producer.KafkaProducer
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class NavNoInntektsmeldingerTest : AbstractRiverTest() {

    @Test
    fun `leser inn svar på forespurte arbeidsgiveropplysninger`() {
        val im = inntektsmelding(
            forespurt = true,
            arsakTilInnsending = "Ny",
        )
        testRapid.sendTestMessage(im)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("arbeidsgiveropplysninger")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `leser inn svar på korrigerte arbeidsgiveropplysninger`() {
        val im = inntektsmelding(
            forespurt = true,
            arsakTilInnsending = "Endring",
        )
        testRapid.sendTestMessage(im)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("korrigerte_arbeidsgiveropplysninger")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `leser inn svar på selvbestemte arbeidsgiveropplysninger`() {
        val im = inntektsmelding(
            forespurt = false,
            arsakTilInnsending = "Hva-som-Helst-Vi-Bryr-Oss-Ikke"
        )
        testRapid.sendTestMessage(im)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("selvbestemte_arbeidsgiveropplysninger")
        assertEquals(OPPRETTET_DATO, testRapid.inspektør.field(0, "@opprettet").asLocalDateTime())
    }

    @Test
    fun `flere inntektsmeldinger forskjellig duplikatkontroll`() {
        testRapid.sendTestMessage(inntektsmelding(
            forespurt = true,
            inntektsmeldingId = "afbb6489-f3f5-4b7d-8689-af1d7b53087a",
            virksomhetsnummer = "virksomhetsnummer",
            arkivreferanse = "arkivreferanse"
        ))
        testRapid.sendTestMessage(inntektsmelding(
            forespurt = true,
            inntektsmeldingId = "66072deb-8586-4fa3-b41a-2e21850fd7db",
            virksomhetsnummer = "virksomhetsnummer",
            arkivreferanse = "arkivreferanse2"
        ))
        assertEquals(2, antallMeldinger(FØDSELSNUMMER))
        assertSendteEvents("arbeidsgiveropplysninger", "arbeidsgiveropplysninger")
    }

    @Language("JSON")
    private fun inntektsmelding(
        forespurt: Boolean,
        arsakTilInnsending: String = "Ny",
        virksomhetsnummer: String = "999999999",
        arbeidstakerFnr: String = FØDSELSNUMMER,
        mottattDato: LocalDateTime = OPPRETTET_DATO,
        vedtaksperiodeId: String = UUID.randomUUID().toString(),
        inntektsmeldingId: String = UUID.randomUUID().toString(),
        arkivreferanse: String = UUID.randomUUID().toString(),

    ) = """
            {
              "avsenderSystem": {"navn": "Hva-som-Helst-Vi-Bryr-Oss-Ikke" },
              "inntektsmeldingId": "$inntektsmeldingId",
              "arsakTilInnsending": "$arsakTilInnsending",
              "virksomhetsnummer": "$virksomhetsnummer", 
              "vedtaksperiodeId": "$vedtaksperiodeId", 
              "arkivreferanse": "$arkivreferanse", 
              "arbeidstakerFnr": "$arbeidstakerFnr",
              "mottattDato": "$mottattDato",
              "format": "Arbeidsgiveropplysninger",
              "forespurt": $forespurt
            }
        """

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
            ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Forespurte)
            ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Korrigerte)
            ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Selvbestemte)
        }
    }
}
