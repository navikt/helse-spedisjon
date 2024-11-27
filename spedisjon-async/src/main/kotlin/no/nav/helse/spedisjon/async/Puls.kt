package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry

internal class Puls(
    rapidsConnection: RapidsConnection,
    private val inntektsmeldingsMediator: InntektsmeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                // pulserer hvert minutt eller hvis noen sender ut 'spedisjon_pulser'-event
                precondition { it.requireAny("@event_name", listOf("minutt", "spedisjon_pulser")) }
            }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        inntektsmeldingsMediator.ekspeder()
    }
}
