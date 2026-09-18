package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import no.nav.sykepenger.libs.logging.loggInfo
import java.util.*

class EkspederingMediator(
    private val dao: EkspederingDao,
    private val rapidsConnection: MessageContext,
) {
    fun videresendMelding(fnr: String, internId: UUID, melding: BeriketMelding) {
        if (!dao.meldingEkspedert(internId)) return duplikatMelding(internId, melding)
        loggInfo("Ekspederer $internId og sender til rapid",
            "fødselsnummer" to fnr,
            "melding" to melding.json)
        rapidsConnection.publish(fnr, melding.json)
    }

    private fun duplikatMelding(internId: UUID, melding: BeriketMelding) {
        loggInfo("Har ekspedert $internId fra før, sender ikke videre til rapid",
            "melding" to melding.json)
    }
}
