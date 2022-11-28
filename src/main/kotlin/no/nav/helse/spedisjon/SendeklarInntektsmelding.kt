package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.MessageContext
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

internal class SendeklarInntektsmelding(
    private val fnr: String,
    private val orgnummer: String,
    val originalMelding: Melding.Inntektsmelding,
    private val berikelse: Berikelse,
    private val mottatt: LocalDateTime
) {

    companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
        internal fun List<SendeklarInntektsmelding>.sorter() = sortedBy { it.mottatt }
    }

    fun json(inntektsmeldingDao: InntektsmeldingDao, inntektsmeldingTimeoutMinutter: Long) = json(
        inntektsmeldingDao.tellInntektsmeldinger(
            fnr,
            orgnummer,
            mottatt.minusMinutes(inntektsmeldingTimeoutMinutter)
        )
    )
    fun send(
        inntektsmeldingDao: InntektsmeldingDao,
        messageContext: MessageContext,
        inntektsmeldingTimeoutMinutter: Long
    ) {
        sikkerlogg.info("Publiserer inntektsmelding med fødselsnummer: $fnr og orgnummer: $orgnummer")
        messageContext.publish(jacksonObjectMapper().writeValueAsString(json(inntektsmeldingDao, inntektsmeldingTimeoutMinutter)))
        inntektsmeldingDao.markerSomEkspedert(originalMelding)
    }

    fun json(antallInntektsmeldinger: Int): JsonNode =
        berikelse.berik(originalMelding).put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)

}