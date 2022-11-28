package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.time.LocalDate

internal class Berikelse(
    private val fødselsdato: LocalDate,
    private val aktørId: String,
    private val støttes: Boolean,
    private val duplikatkontroll: String
) {

    companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()
    }

    internal fun behandle(melding: Melding, berikelseDao: BerikelseDao, onBeriketMelding: (JsonNode) -> Unit) {
        val eventName = melding.type
        if(støttes) {
            val beriketMelding = berik(melding)
            if (eventName != "inntektsmelding"){
                onBeriketMelding(beriketMelding)
            }
        }
        else {
            sikkerLogg.info("Personen støttes ikke $aktørId")
        }
        berikelseDao.behovBesvart(duplikatkontroll, lagringsJson(eventName))
    }

    private fun berik(melding: Melding): JsonNode {
        val json = melding.jsonNode()
        json as ObjectNode
        val eventName = json["@event_name"].asText()
        json.setAll<ObjectNode>(løsningJson(eventName))
        sikkerLogg.info("publiserer $eventName for ${melding.fødselsnummer()}: \n$json")
        return json
    }

    private fun lagringsJson(eventName: String) =
        løsningJson(eventName).put("støttes", støttes)

    private fun løsningJson(eventName: String) =
        objectMapper.createObjectNode().put("fødselsdato", fødselsdato.toString()).put(
            aktørIdFeltnavn(eventName), aktørId)

    internal fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"

}