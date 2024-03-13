package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class Puls(
    rapidsConnection: RapidsConnection,
    private val inntektsmeldingsMediator: InntektsmeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                // pulserer hvert minutt eller hvis noen sender ut 'spedisjon_pulser'-event
                validate { it.demandAny("@event_name", listOf("minutt", "spedisjon_pulser")) }
            }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        inntektsmeldingsMediator.ekspeder(context)
    }
}
