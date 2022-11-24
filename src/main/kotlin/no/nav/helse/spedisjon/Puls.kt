package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime

internal class Puls(
    rapidsConnection: RapidsConnection,
    schedule: Duration,
    private val inntektsmeldingsMediator: InntektsmeldingMediator
) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(Puls::class.java)
        private val secureLogger = LoggerFactory.getLogger("tjenestekall")
    }

    private var lastReportTime = LocalDateTime.MIN
    private val påminnelseSchedule = { lastReportTime: LocalDateTime ->
        lastReportTime < LocalDateTime.now().minusSeconds(schedule.toSeconds())
    }

    init {
        River(rapidsConnection).register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (!påminnelseSchedule(lastReportTime)) return
        publiserInntektsmeldinger(context)
        lastReportTime = LocalDateTime.now()
    }

    private fun publiserInntektsmeldinger(context: MessageContext) {
        secureLogger.info("henter inntektsmeldinger som potensielt skal publiseres")
        inntektsmeldingsMediator.republiser(context)
    }
}
