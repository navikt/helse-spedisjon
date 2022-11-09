package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.spedisjon.Personidentifikator.Companion.fødselsdatoOrNull
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val berikelseDao: BerikelseDao,
) {
    internal companion object {
        private val logg = LoggerFactory.getLogger(MeldingMediator::class.java)
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()
        private val meldingsteller = Counter.build("melding_totals", "Antall meldinger mottatt")
            .labelNames("type")
            .register()
        private val unikteller = Counter.build("melding_unik_totals", "Antall unike meldinger mottatt")
            .labelNames("type")
            .register()
        private val sendtteller = Counter.build("melding_sendt_totals", "Antall meldinger sendt")
            .labelNames("type")
            .register()

        internal fun berik(melding: Pair<String, JsonNode>, fødselsdato: LocalDate, aktørId: String): JsonNode {
            val json = melding.second
            json as ObjectNode
            val eventName = json["@event_name"].asText()
            json.setAll<ObjectNode>(løsningJson(eventName, fødselsdato, aktørId))
            sikkerLogg.info("publiserer $eventName for ${melding.first}: \n$json")
            return json
        }
        internal fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"

        private fun løsningJson(eventName: String, fødselsdato: LocalDate, aktørId: String) =
            objectMapper.createObjectNode().put("fødselsdato", fødselsdato.toString()).put(aktørIdFeltnavn(eventName), aktørId)
    }

    private var messageRecognized = false
    private val riverErrors = mutableListOf<String>()

    fun beforeMessage() {
        messageRecognized = false
        riverErrors.clear()
    }

    fun onRiverError(error: String) {
        riverErrors.add(error)
    }

    fun onMelding(melding: Melding, context: MessageContext) {
        messageRecognized = true
        meldingsteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        sendBehovÈnGang(melding.fødselsnummer(), listOf("aktørId", "fødselsdato"), melding.duplikatkontroll(), context)
        unikteller.labels(melding.type).inc()
        sendtteller.labels(melding.type).inc()
    }

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun sendBehovÈnGang(fødselsnummer: String, behov: List<String>, duplikatkontroll: String, context: MessageContext) {
        if (berikelseDao.behovErEtterspurt(duplikatkontroll)) return // Om om vi allerede har etterspurt behov gjør vi det ikke på ny
        sendBehov(fødselsnummer, behov, duplikatkontroll, context)
    }

    private fun sendBehov(fødselsnummer: String, behov: List<String>, duplikatkontroll: String, context: MessageContext) {
        context.publish(fødselsnummer,
            JsonMessage.newNeed(behov = listOf("HentPersoninfoV3"),
                map = mapOf("HentPersoninfoV3" to mapOf(
                    "ident" to fødselsnummer,
                    "attributter" to behov,
                ), "spedisjonMeldingId" to duplikatkontroll)).toJson())
        berikelseDao.behovEtterspurt(fødselsnummer, duplikatkontroll, behov, LocalDateTime.now())
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
        val (fødselsnummer, json) = melding
        val eventName = json["@event_name"].asText()
        if (fødselsdato != fødselsnummer.fødselsdatoOrNull()) {
            sikkerLogg.info("publiserer $eventName for $fødselsnummer hvor fødselsdato ($fødselsdato) ikke kan utledes fra personidentifikator")
        }
        if(støttes) {
            context.publish(fødselsnummer, berik(melding, fødselsdato, aktørId).toString())
        }
        else {
            sikkerLogg.info("Personen støttes ikke $aktørId")
        }
        berikelseDao.behovBesvart(duplikatkontroll, løsningJson(eventName, fødselsdato, aktørId))
    }

    fun retryBehov(opprettetFør: LocalDateTime, context:MessageContext) {
        val ubesvarteBehov = berikelseDao.ubesvarteBehov(opprettetFør)
        logg.info("Er ${ubesvarteBehov.size} ubesvarte behov som er opprettet før $opprettetFør")
        ubesvarteBehov.forEach { ubesvartBehov ->
            ubesvartBehov.logg(logg)
            sendBehov(ubesvartBehov.fnr, ubesvartBehov.behov, ubesvartBehov.duplikatkontroll, context)
        }
    }
}
