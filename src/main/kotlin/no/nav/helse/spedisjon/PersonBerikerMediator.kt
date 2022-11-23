package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.spedisjon.Personidentifikator.Companion.fødselsdatoOrNull
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
        fødselsdato: LocalDate,
        aktørId: String,
        støttes: Boolean,
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
        val eventName = melding.type
        if (fødselsdato != melding.fødselsnummer().fødselsdatoOrNull()) {
            sikkerLogg.info("publiserer $eventName for ${melding.fødselsnummer()} hvor fødselsdato ($fødselsdato) ikke kan utledes fra personidentifikator")
        }
        val beriketMelding = berik(melding, fødselsdato, aktørId)
        if(støttes) {
            meldingMediator.onPersoninfoBerikelse(context, melding.fødselsnummer(), beriketMelding)
        }
        else {
            sikkerLogg.info("Personen støttes ikke $aktørId")
        }
        berikelseDao.behovBesvart(duplikatkontroll, løsningJson(eventName, fødselsdato, aktørId))
    }
}