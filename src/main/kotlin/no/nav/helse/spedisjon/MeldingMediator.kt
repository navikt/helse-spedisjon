package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.isMissingOrNull
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val berikelseDao: BerikelseDao,
    private val aktørregisteretClient: AktørregisteretClient,
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
        private val fnrteller = Counter.build("melding_oppslag_fnr_totals", "Antall ganger fnr er slått opp")
            .labelNames("type")
            .register()
        internal fun berik(melding: Pair<String, JsonNode>, fødselsdato: LocalDate, aktørId: String): JsonNode {
            val json = melding.second
            json as ObjectNode
            val eventName = json["@event_name"].asText()
            val beriketEvent = beriketEventName(eventName)
            json.put("@event_name", beriketEvent)
            json.setAll<ObjectNode>(løsningJson(eventName, fødselsdato, aktørId))
            sikkerLogg.info("publiserer $beriketEvent for ${melding.first}: \n$json")
            return json
        }
        internal fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"

        private fun beriketEventName(eventName: String) = if (eventName in listOf("ny_søknad", "inntektsmelding")) eventName else "${eventName}_beriket"

        private fun løsningJson(eventName: String, fødselsdato: LocalDate, aktørId: String) =
            objectMapper.createObjectNode().put("fødselsdato", fødselsdato.toString()).put(aktørIdFeltnavn(eventName), aktørId)
    }

    private var fnroppslag = false
    private var messageRecognized = false
    private val riverErrors = mutableListOf<String>()

    fun beforeMessage() {
        fnroppslag = false
        messageRecognized = false
        riverErrors.clear()
    }

    fun onPacket(packet: JsonMessage, aktørIdfelt: String, fødselsnummerfelt: String) {
        messageRecognized = true
        packet.putIfAbsent(fødselsnummerfelt) {
            fnroppslag = true
            sikkerLogg.info("gjør oppslag på fnr for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentFødselsnummer(packet[aktørIdfelt].asText())
        }

        packet.putIfAbsent(aktørIdfelt) {
            sikkerLogg.info("gjør oppslag på aktørId for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentAktørId(packet[fødselsnummerfelt].asText())
        }
    }

    fun onRiverError(error: String) {
        riverErrors.add(error)
    }

    fun onMelding(melding: Melding, context: MessageContext) {
        meldingsteller.labels(melding.type).inc()
        if (fnroppslag) fnrteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        unikteller.labels(melding.type).inc()
        sendtteller.labels(melding.type).inc()
        context.publish(melding.fødselsnummer(), melding.json())
    }

    fun onMeldingAsync(melding: Melding, context: MessageContext) {
        messageRecognized = true
        meldingsteller.labels(melding.type).inc()
        // Sender alltid behov selv om vi har lagret meldingen før. Vi sender uansett kun ut melding ved første løsning
        sendBehov(melding.fødselsnummer(), listOf("aktørId", "fødselsdato"), melding.duplikatkontroll(), context)
        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        unikteller.labels(melding.type).inc()
        sendtteller.labels(melding.type).inc()
    }

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
        if (!this[key].isMissingOrNull()) return
        block()?.also { this[key] = it }
    }

    fun sendBehov(fødselsnummer: String, behov: List<String>, duplikatkontroll: String, context: MessageContext) {
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
        context.publish(melding.first, berik(melding, fødselsdato, aktørId).toString())
        val eventName = melding.second["@event_name"].asText()
        berikelseDao.behovBesvart(duplikatkontroll, løsningJson(eventName, fødselsdato, aktørId))
    }

    fun retryBehov(opprettetFør: LocalDateTime, context:MessageContext) {
        berikelseDao.ubesvarteBehov(opprettetFør).forEach { ubesvartBehov ->
            logg.info("Sender ut nytt behov for duplikatkontroll=${ubesvartBehov.duplikatkontroll}")
            sendBehov(ubesvartBehov.fnr, ubesvartBehov.behov, ubesvartBehov.duplikatkontroll, context)
        }
    }
}
