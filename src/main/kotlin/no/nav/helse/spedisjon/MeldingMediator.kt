package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.isMissingOrNull
import org.slf4j.LoggerFactory

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val aktørregisteretClient: AktørregisteretClient,
    private val streamToRapid: Boolean
) {
    private companion object {
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val log = LoggerFactory.getLogger(MeldingMediator::class.java)
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
    }

    private var fnroppslag = false
    private var messageRecognized = false
    private val riverErrors = mutableListOf<String>()

    init {
        if (!streamToRapid) log.warn("Sender ikke meldinger videre til rapid")
    }

    fun beforeMessage(message: String) {
        fnroppslag = false
        messageRecognized = false
        riverErrors.clear()
    }

    fun onPacket(packet: JsonMessage, aktørIdfelt: String, fødselsnummerfelt: String) {
        messageRecognized = true
        packet.putIfAbsent(fødselsnummerfelt) {
            fnroppslag = true;
            sikkerLogg.info("gjør oppslag på fnr for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentFødselsnummer(packet[aktørIdfelt].asText())}

        packet.putIfAbsent(aktørIdfelt) {
            sikkerLogg.info("gjør oppslag på aktørId for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentAktørId(packet[fødselsnummerfelt].asText())}
    }

    fun onRiverError(error: String) {
        riverErrors.add(error)
    }

    fun onMelding(melding: Melding, context: MessageContext) {
        meldingsteller.labels(melding.type).inc()
        if (fnroppslag) fnrteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        unikteller.labels(melding.type).inc()
        if (!streamToRapid) return
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

    // Midlertidig kode mens vi tester i dev for å ikke behandle søknader med type != ARBEIDSTAKERE på det nye topicet
    // Når vi er 100% over på aiven kan denne filtreringen skje i valideringen i riveren
    fun søknadErRelevant(packet: JsonMessage): Boolean {
        val type: String? = packet["type"].takeUnless(JsonNode::isMissingOrNull)?.asText()
        if (type != null && type != "ARBEIDSTAKERE") {
            sikkerLogg.info("Søknad hadde ikke type ARBEIDSTAKERE: ${packet.toJson()}")
            return false
        }
        return true
    }
}
