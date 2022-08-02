package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.isMissingOrNull
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.LocalDateTime

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val aktørregisteretClient: AktørregisteretClient,
) {
    internal companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
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
            val beriketEvent = eventName + "_beriket"
            json.put("fødselsdato", fødselsdato.toString())
            json.put(aktørIdFeltnavn(eventName), aktørId)
            json.put("@event_name", beriketEvent)
            sikkerLogg.info("publiserer $beriketEvent for ${melding.first}: \n$json")
            return json
        }
        private fun aktørIdFeltnavn(eventName: String) = if (eventName == "inntektsmelding") "arbeidstakerAktorId" else "aktorId"
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

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
        if (!this[key].isMissingOrNull()) return
        block()?.also { this[key] = it }
    }

    fun sendBehov(fødselsnummer: String, behovsliste: List<String>, behovskilde: String, context: MessageContext) {
        context.publish(fødselsnummer,
            JsonMessage.newNeed(behov = listOf("HentPersoninfoV3"),
                map = mapOf("HentPersoninfoV3" to mapOf(
                    "ident" to fødselsnummer,
                    "attributter" to behovsliste,
                ), "spedisjonMeldingId" to behovskilde)).toJson())
    }

    fun onPersoninfoBerikelse(
        duplikatkontroll: String,
        fødselsdato: LocalDate,
        aktørId: String,
        context: MessageContext
    ) {
        val melding = meldingDao.hent(duplikatkontroll)
        if (melding == null) {
            sikkerLogg.warn("Mottok personinfoberikelse med duplikatkontroll=$duplikatkontroll som vi ikke fant i databasen")
            return
        }
        context.publish(melding.first, berik(melding, fødselsdato, aktørId).toString())
    }

    fun retryBehov(tidligereEnn: LocalDateTime, context:MessageContext) {
        val fattigeDokumenter = finnDokumenterUtenBerikelse(tidligereEnn)
        fattigeDokumenter.forEach { sendBehov(it.fnr, emptyList(), it.duplikatkontroll, context) }
    }

    fun finnDokumenterUtenBerikelse(tidligereEnn: LocalDateTime): List<UbesvartBehov> = emptyList()

}
