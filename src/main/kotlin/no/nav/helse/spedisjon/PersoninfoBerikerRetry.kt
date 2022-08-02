package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.time.Duration
import java.time.LocalDateTime

internal class PersoninfoBerikerRetry(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
) : River.PacketListener {
    private val publiseringintervall: Duration = Duration.ofSeconds(30)
    private val timeout: Duration = Duration.ofMinutes(15)
    private var sistPublisert: LocalDateTime = LocalDateTime.MIN

    init {
        River(rapidsConnection).register(this)
    }

    private fun skalKjøreRetry() = sistPublisert < LocalDateTime.now().minus(publiseringintervall)
    private fun harKjørtRetry() {
        sistPublisert = LocalDateTime.now()
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (skalKjøreRetry()) {
            meldingMediator.retryBehov(tidligereEnn = LocalDateTime.now().minus(timeout), context)
            harKjørtRetry()
        }
    }
}