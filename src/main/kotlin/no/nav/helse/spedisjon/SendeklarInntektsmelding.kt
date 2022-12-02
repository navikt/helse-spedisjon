package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
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

    fun send(
        inntektsmeldingDao: InntektsmeldingDao,
        messageContext: MessageContext,
        inntektsmeldingTimeoutSekunder: Long
    ) {
        berikelse.behandle(originalMelding) { beriketMelding ->
            sikkerlogg.info("Ekspederer inntektsmelding med fødselsnummer: $fnr og orgnummer: $orgnummer")
            val json = flaggFlereInntektsmeldinger((beriketMelding as ObjectNode), tell(inntektsmeldingDao, inntektsmeldingTimeoutSekunder))
            messageContext.publish(fnr, jacksonObjectMapper().writeValueAsString(json))
        }

        inntektsmeldingDao.markerSomEkspedert(originalMelding)
    }

    private fun tell(inntektsmeldingDao: InntektsmeldingDao, inntektsmeldingTimeoutMinutter: Long) =
        inntektsmeldingDao.tellInntektsmeldinger(fnr, orgnummer, mottatt.minusSeconds(inntektsmeldingTimeoutMinutter))

    fun flaggFlereInntektsmeldinger(beriketMelding: ObjectNode, antallInntektsmeldinger: Int): JsonNode =
        beriketMelding.put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)

}