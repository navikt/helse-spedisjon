package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.asLocalDate
import no.nav.helse.rapids_rivers.asOptionalLocalDate
import org.slf4j.LoggerFactory
import java.time.LocalDate

internal class Berikelse(
    private val fødselsdato: LocalDate,
    private val dødsdato: LocalDate?,
    private val aktørId: String,
    private val støttes: Boolean,
    private val historiskeFolkeregisteridenter: List<String>,
    private val duplikatkontroll: String
) {

    companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()

        fun les(jsonNode: JsonNode, duplikatkontroll: String): Berikelse{
            return Berikelse(
                fødselsdato = jsonNode["fødselsdato"].asLocalDate(),
                dødsdato = jsonNode.path("dødsdato").asOptionalLocalDate(),
                aktørId = jsonNode["aktorId"]?.asText()?: jsonNode["arbeidstakerAktorId"].asText(),
                støttes = jsonNode["støttes"].asBoolean(true),
                historiskeFolkeregisteridenter = jsonNode.path("historiskeFolkeregisteridenter").map(JsonNode::asText),
                duplikatkontroll = duplikatkontroll
            )
        }
    }

    internal fun behandle(melding: Melding, onBeriketMelding: (JsonNode) -> Unit) {
        if(støttes) {
            val beriketMelding = berik(melding)
            onBeriketMelding(beriketMelding)
        }
        else {
            sikkerLogg.info("Personen støttes ikke $aktørId")
        }
    }

    internal fun lagre(berikelseDao: BerikelseDao, eventName: String) {
        berikelseDao.behovBesvart(duplikatkontroll, lagringsJson(eventName))
    }

    internal fun berik(melding: Melding): ObjectNode {
        val json = melding.jsonNode()
        json as ObjectNode
        val eventName = json["@event_name"].asText()
        json.setAll<ObjectNode>(løsningJson(eventName))
        return json
    }

    private fun lagringsJson(eventName: String) =
        løsningJson(eventName).put("støttes", støttes)

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