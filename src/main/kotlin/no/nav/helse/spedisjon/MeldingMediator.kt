package no.nav.helse.spedisjon

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
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

    private val messageProblems = mutableListOf<Pair<String, MessageProblems>>()
    private var fnroppslag = false

    init {
        if (!streamToRapid) log.warn("Sender ikke meldinger videre til rapid")
    }

    fun beforeMessage(message: String, numberOfRivers: Int) {
        fnroppslag = false
    }

    fun onPacket(packet: JsonMessage, aktørIdfelt: String, fødselsnummerfelt: String) {
        packet.putIfAbsent(fødselsnummerfelt) {
            fnroppslag = true;
            sikkerLogg.info("gjør oppslag på fnr for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentFødselsnummer(packet[aktørIdfelt].asText())}
    }

    fun onRiverError(river: String, problems: MessageProblems) {
        messageProblems.add(river to problems)
    }

    fun onMelding(melding: Melding, context: RapidsConnection.MessageContext) {
        meldingsteller.labels(melding.type).inc()
        if (fnroppslag) fnrteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return
        unikteller.labels(melding.type).inc()
        if (!streamToRapid) return
        sendtteller.labels(melding.type).inc()
        context.send(melding.fødselsnummer(), melding.json())
    }

    fun afterMessage(message: String, numberOfRivers: Int) {
        logProblems(message, numberOfRivers)
        messageProblems.clear()
    }

    private fun logProblems(message: String, numberOfRivers: Int) {
        if (isRapidEvent(message)) return
        if (messageProblems.size != numberOfRivers) return
        sikkerLogg.info(
            "Kunne ikke forstå melding:\n{}\n\nProblemer:\n{}",
            message,
            messageProblems.joinToString(separator = "\n\n") {
                "${it.first}:\n${it.second}"
            })
    }

    private fun isRapidEvent(message: String): Boolean {
        return try {
            MessageProblems(message).also {
                JsonMessage(message, it).apply { forbid("@event_name") }
            }.hasErrors()
        } catch (err: MessageProblems.MessageException) { false }
    }

    private fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
        if (!this[key].isMissingOrNull()) return
        block()?.also { this[key] = it }
    }
}
