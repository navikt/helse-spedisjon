package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class AndreHendelser(rapidsConnection: RapidsConnection, private val problemsCollector: ProblemsCollector) :
    River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.requireKey("@event_name") }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        problemsCollector.add("Rapid-hendelse", problems)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        // ignore
    }
}
