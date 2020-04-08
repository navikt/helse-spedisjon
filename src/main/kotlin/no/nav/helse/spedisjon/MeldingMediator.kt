package no.nav.helse.spedisjon

import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.isMissingOrNull
import org.slf4j.LoggerFactory

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val aktørregisteretClient: AktørregisteretClient,
    private val streamToRapid: Boolean
) {
    private companion object {
        private val log = LoggerFactory.getLogger(MeldingMediator::class.java)
        private val meldingsteller = Counter.build("melding_totals", "Antall meldinger mottatt")
            .labelNames("type")
            .register()
        private val unikteller = Counter.build("melding_unik_totals", "Antall unike meldinger mottatt")
            .labelNames("type")
            .register()
    }

    init {
        if (!streamToRapid) log.warn("Sender ikke meldinger videre til rapid")
    }

    fun onPacket(packet: JsonMessage, aktørIdfelt: String, fødselsnummerfelt: String) {
        packet.putIfAbsent(fødselsnummerfelt) { aktørregisteretClient.hentFødselsnummer(packet[aktørIdfelt].asText())}
    }

    fun onMelding(melding: Melding, context: RapidsConnection.MessageContext) {
        meldingsteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return
        unikteller.labels(melding.type).inc()
        if (!streamToRapid) return
        context.send(melding.fødselsnummer(), melding.json())
    }

    private fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
        if (!this[key].isMissingOrNull()) return
        block()?.also { this[key] = it }
    }
}
