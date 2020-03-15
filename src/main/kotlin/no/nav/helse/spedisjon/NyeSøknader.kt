package no.nav.helse.spedisjon

import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.slf4j.LoggerFactory

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
            validate { it.requireKey("id", "sykmeldingId", "fom", "tom") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val nySøknad = Melding.NySøknad(packet)
        if (!meldingDao.leggInn(nySøknad)) return log.error("Duplikat nySøknad: {} {} ",
            keyValue("duplikatkontroll", nySøknad.duplikatkontroll()),
            keyValue("melding", nySøknad.json()))

//            context.send(nySøknad.fødselsnummer(), nySøknad.json())
    }
}
