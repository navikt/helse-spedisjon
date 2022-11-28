package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.MessageContext
import org.slf4j.LoggerFactory
import java.time.LocalDate

internal class PersonBerikerMediator(
    private val meldingDao: MeldingDao,
    private val berikelseDao: BerikelseDao,
    private val meldingMediator: MeldingMediator
    ) {

    internal companion object {
        private val logg = LoggerFactory.getLogger(PersonBerikerMediator::class.java)
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()

        internal fun berik(melding: Melding, fødselsdato: LocalDate, aktørId: String): JsonNode {
            val json = melding.jsonNode()
            json as ObjectNode
            val eventName = json["@event_name"].asText()
            json.setAll<ObjectNode>(løsningJson(eventName, fødselsdato, aktørId))
            sikkerLogg.info("publiserer $eventName for ${melding.fødselsnummer()}: \n$json")
            return json
        }

        private fun løsningJson(eventName: String, fødselsdato: LocalDate, aktørId: String) =
            objectMapper.createObjectNode().put("fødselsdato", fødselsdato.toString()).put(
                aktørIdFeltnavn(eventName), aktørId)

        internal fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"

    }

    fun onPersoninfoBerikelse(
        duplikatkontroll: String,
        berikelse: Berikelse,
        context: MessageContext
    ) {
        val melding = meldingDao.hent(duplikatkontroll)
        if (melding == null) {
            logg.warn("Mottok personinfoberikelse med duplikatkontroll=$duplikatkontroll som vi ikke fant i databasen")
            return
        }
        if (berikelseDao.behovErBesvart(duplikatkontroll)) {
            logg.info("Behov er allerede besvart for duplikatkontroll=$duplikatkontroll")
            return
        }
        berikelse.behandle(melding, berikelseDao) {
                beriketMelding -> meldingMediator.onPersoninfoBerikelse(context, melding.fødselsnummer(), beriketMelding)
        }
    }
}