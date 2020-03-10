package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import no.nav.helse.rapids_rivers.asLocalDateTime
import no.nav.helse.spedisjon.MeldingDao.MeldingType.NY_SØKNAD
import org.slf4j.LoggerFactory
import java.util.*

internal class NyeSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao
) : River.PacketListener {

    private companion object {
        private val log = LoggerFactory.getLogger(NyeSøknader::class.java)
    }

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("fnr", "aktorId", "arbeidsgiver.orgnummer", "opprettet", "soknadsperioder") }
            validate { it.requireValue("status", "NY") }
            validate { it.requireKey("sykmeldingId", "fom", "tom") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val opprettet = packet["opprettet"].asLocalDateTime()
        packet["@event_name"] = "ny_søknad"
        packet["@id"] = UUID.randomUUID()
        packet["@opprettet"] = opprettet

        val fødselsnummer = packet["fnr"].asText()
        meldingDao.leggInn(NY_SØKNAD, fødselsnummer, packet.toJson(), opprettet)
        context.send(fødselsnummer, packet.toJson())
    }
}
