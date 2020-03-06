package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import org.slf4j.LoggerFactory

internal class SendteSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao
) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(SendteSøknader::class.java)
    }

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("fnr", "aktorId", "arbeidsgiver.orgnummer", "opprettet", "soknadsperioder") }
            validate { it.requireValue("status", "SENDT") }
            validate { it.requireKey("id", "sendtNav", "fom", "tom", "egenmeldinger", "fravar") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val sendtNav = packet["sendtNav"].asLocalDateTime()
        packet["@event_name"] = "sendt_søknad"
        packet["@opprettet"] = sendtNav

        meldingDao.leggInn(packet.toJson(), sendtNav)
        context.send(packet["fnr"].asText(), packet.toJson())
    }
}
