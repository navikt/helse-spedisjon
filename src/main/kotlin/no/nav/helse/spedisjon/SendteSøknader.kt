package no.nav.helse.spedisjon

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
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
        val sendtSøknad = Melding.SendtSøknad(packet)
        if (!meldingDao.leggInn(sendtSøknad)) return log.error("Duplikat sendtSøknad: {} {} ",
            keyValue("duplikatkontroll", sendtSøknad.duplikatkontroll()),
            keyValue("melding", sendtSøknad.json()))

//        context.send(sendtSøknad.fødselsnummer(), sendtSøknad.json())

    }
}
