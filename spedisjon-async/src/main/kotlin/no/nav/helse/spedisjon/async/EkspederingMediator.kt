package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import java.util.*

class EkspederingMediator(
    private val dao: EkspederingDao,
    private val rapidsConnection: MessageContext,
) {
    fun videresendMelding(fnr: String, internId: UUID, melding: BeriketMelding) {
        if (!dao.meldingEkspedert(internId)) return duplikatMelding(internId, melding)
        "Ekspederer {} og sender til rapid".also {
            logg.info(it, kv("internId", internId))
            sikkerlogg.info("${it}:\n${melding.json}", kv("internId", internId))
        }
        rapidsConnection.publish(fnr, melding.json)
    }

    private fun duplikatMelding(internId: UUID, melding: BeriketMelding) {
        "Har ekspedert {} fra før, sender ikke videre til rapid".also {
            logg.info(it, kv("internId", internId))
            sikkerlogg.info("${it}:\n${melding.json}", kv("internId", internId))
        }
    }

    private companion object {
        private val logg = LoggerFactory.getLogger(EkspederingMediator::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
}