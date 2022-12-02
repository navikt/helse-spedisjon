package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import java.net.InetAddress
import java.time.Duration
import java.time.LocalDateTime

internal class Puls(
    rapidsConnection: RapidsConnection,
    schedule: Duration,
    private val inntektsmeldingsMediator: InntektsmeldingMediator,
    private val electectorPath: String?
) : River.PacketListener {

    private var lastReportTime = LocalDateTime.MIN
    private val påminnelseSchedule = { lastReportTime: LocalDateTime ->
        lastReportTime < LocalDateTime.now().minusSeconds(schedule.toSeconds())
    }

    init {
        River(rapidsConnection).register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        if (!påminnelseSchedule(lastReportTime)) return
        if (isLeader()) {
            ekspederInntektsmeldinger(context)
            lastReportTime = LocalDateTime.now()
        }
    }

    private fun isLeader(): Boolean {
        val leaderHost = jacksonObjectMapper().readTree(electectorPath)["name"].asText()
        val host = InetAddress.getLocalHost().hostName
        return leaderHost == host
    }

    private fun ekspederInntektsmeldinger(context: MessageContext) {
        inntektsmeldingsMediator.ekspeder(context)
    }
}
