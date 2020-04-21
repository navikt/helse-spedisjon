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

    private var fnroppslag = false
    private var messageRecognized = false
    private val riverSevereErrors = mutableListOf<Pair<String, MessageProblems>>()
    private val riverErrors = mutableListOf<Pair<String, MessageProblems>>()

    init {
        if (!streamToRapid) log.warn("Sender ikke meldinger videre til rapid")
    }

    fun beforeMessage(message: String) {
        fnroppslag = false
        messageRecognized = false
        riverSevereErrors.clear()
        riverErrors.clear()
    }

    fun onPacket(packet: JsonMessage, aktørIdfelt: String, fødselsnummerfelt: String) {
        messageRecognized = true
        packet.putIfAbsent(fødselsnummerfelt) {
            fnroppslag = true;
            sikkerLogg.info("gjør oppslag på fnr for melding:\n${packet.toJson()}")
            aktørregisteretClient.hentFødselsnummer(packet[aktørIdfelt].asText())}
    }

    fun onRiverError(riverName: String, problems: MessageProblems) {
        riverErrors.add(riverName to problems)
    }

    fun onRiverSevere(riverName: String, error: MessageProblems.MessageException) {
        riverSevereErrors.add(riverName to error.problems)
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

    fun afterMessage(message: String) {
        if (messageRecognized) return
        if (riverErrors.isNotEmpty()) return sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n") { "${it.first}:\n${it.second}" }}")
        sikkerLogg.debug("ukjent melding:\n\t$message\n\nProblemer:\n${riverSevereErrors.joinToString(separator = "\n") { "${it.first}:\n${it.second}" }}")
    }

    private fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
        if (!this[key].isMissingOrNull()) return
        block()?.also { this[key] = it }
    }
}
