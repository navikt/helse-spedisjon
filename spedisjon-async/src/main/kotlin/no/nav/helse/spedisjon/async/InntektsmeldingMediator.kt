package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.speed.SpeedClient
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

internal class InntektsmeldingMediator(
    private val speedClient: SpeedClient,
    private val inntektsmeldingDao: InntektsmeldingDao,
    private val dokumentAliasProducer: DokumentAliasProducer,
    private val inntektsmeldingTimeoutSekunder: Long = 1
) {

    private companion object {
        private val logg = LoggerFactory.getLogger(Puls::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        private val objectmapper = jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    fun lagreInntektsmelding(
        inntektsmelding: Melding.Inntektsmelding,
        messageContext: MessageContext,
        ønsketPublisert: LocalDateTime = LocalDateTime.now().plusSeconds(inntektsmeldingTimeoutSekunder),
        mottatt: LocalDateTime = LocalDateTime.now()
    ) {
        if (!inntektsmeldingDao.leggInn(inntektsmelding, ønsketPublisert, mottatt)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen

        if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
            logg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            sikkerlogg.info("ekspederer inntektsmeldinger på direkten fordi vi er i dev")
            ekspeder(messageContext)
        }
    }

    fun ekspeder(messageContext: MessageContext) {
        inntektsmeldingDao.hentSendeklareMeldinger(inntektsmeldingTimeoutSekunder) { inntektsmelding, antallInntektsmeldingerMottatt ->
            Personinformasjon.Companion.berikMeldingOgBehandleDen(speedClient, inntektsmelding.melding) { berikelse ->
                ekspederInntektsmelding(messageContext, berikelse, inntektsmelding, antallInntektsmeldingerMottatt)
            }
        }
    }

    private fun ekspederInntektsmelding(messageContext: MessageContext, berikelse: Berikelse, inntektsmelding: SendeklarInntektsmelding, antallInntektsmeldingMottatt: Int) {
        val beriketMelding = berikelse.berik(inntektsmelding.melding)
        sikkerlogg.info("Ekspederer inntektsmelding med fødselsnummer: ${inntektsmelding.fnr} og orgnummer: ${inntektsmelding.orgnummer}")
        val beriketMeldingMedFlagg = flaggFlereInntektsmeldinger(beriketMelding, antallInntektsmeldingMottatt)
        dokumentAliasProducer.send(inntektsmelding.melding)
        messageContext.publish(inntektsmelding.fnr, beriketMeldingMedFlagg.json)
    }

    private fun flaggFlereInntektsmeldinger(beriketMelding: BeriketMelding, antallInntektsmeldinger: Int) =
        beriketMelding.copy(
            json = (objectmapper.readTree(beriketMelding.json) as ObjectNode).apply {
                put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)
            }.toString()
        )
}