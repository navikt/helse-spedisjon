package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.asLocalDate
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime

internal class SendeklarInntektsmelding(
    private val fnr: String,
    private val orgnummer: String,
    val originalMelding: Melding.Inntektsmelding,
    private val berikelse: JsonNode,
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

    fun json(antallInntektsmeldinger: Int): JsonNode {
        val json = originalMelding.jsonNode()
        json as ObjectNode
        json.setAll<ObjectNode>(løsningJson(berikelse["fødselsdato"].asLocalDate(), berikelse["arbeidstakerAktorId"].asText()))
        json.put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)
        return json
    }

    private fun løsningJson(fødselsdato: LocalDate, aktørId: String) =
        jacksonObjectMapper().createObjectNode().put("fødselsdato", fødselsdato.toString()).put(
            "arbeidstakerAktorId", aktørId)


}