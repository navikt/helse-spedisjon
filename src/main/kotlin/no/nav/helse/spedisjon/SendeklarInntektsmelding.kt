package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.asLocalDate
import java.time.LocalDate
import java.time.LocalDateTime

internal class SendeklarInntektsmelding(
    private val fnr: String,
    private val orgnummer: String,
    val originalMelding: Melding.Inntektsmelding,
    private val berikelse: JsonNode,
    private val mottatt: LocalDateTime
) {

    fun json(inntektsmeldingDao: InntektsmeldingDao) = json(inntektsmeldingDao.tellInntektsmeldinger(fnr, orgnummer, mottatt))
    fun send(inntektsmeldingDao: InntektsmeldingDao, messageContext: MessageContext) {

    }

    fun json(antallInntektsmeldinger: Int): JsonNode {
        val json = originalMelding.jsonNode()
        json as ObjectNode
        json.setAll<ObjectNode>(løsningJson(berikelse["fødselsdato"].asLocalDate(), berikelse["aktørId"].asText()))
        json.put("harFlereInntektsmeldinger", antallInntektsmeldinger > 1)
        return json
    }

    private fun løsningJson(fødselsdato: LocalDate, aktørId: String) =
        jacksonObjectMapper().createObjectNode().put("fødselsdato", fødselsdato.toString()).put(
            "arbeidstakerAktorId", aktørId)


}