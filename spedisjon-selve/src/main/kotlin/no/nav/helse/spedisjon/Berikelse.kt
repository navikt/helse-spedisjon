package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDate

internal class Berikelse(
    private val fødselsdato: LocalDate,
    private val dødsdato: LocalDate?,
    private val aktørId: String,
    private val historiskeFolkeregisteridenter: List<String>
) {

    companion object {
        private val objectMapper = jacksonObjectMapper()
    }

    internal fun berik(melding: Melding): ObjectNode {
        val json = melding.jsonNode()
        json as ObjectNode
        val eventName = json["@event_name"].asText()
        json.setAll<ObjectNode>(løsningJson(eventName))
        return json
    }

    private fun løsningJson(eventName: String) =
        objectMapper.createObjectNode()
            .put("fødselsdato", fødselsdato.toString())
            .apply {
                if (dødsdato == null) putNull("dødsdato")
                else put("dødsdato", "$dødsdato")
            }
            .put(aktørIdFeltnavn(eventName), aktørId).apply {
                val historiskeFolkeregisteridenterLøsning = withArray("historiskeFolkeregisteridenter")
                historiskeFolkeregisteridenter.forEach {ident ->
                    historiskeFolkeregisteridenterLøsning.add(ident)
                }
            }

    internal fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"

}