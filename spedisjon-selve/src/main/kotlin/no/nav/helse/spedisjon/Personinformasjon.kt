package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.retry.retry
import com.github.navikt.tbd_libs.speed.HistoriskeIdenterResponse
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.PersonResponse
import com.github.navikt.tbd_libs.speed.SpeedClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory

data class Personinformasjon(
    val personinfo: PersonResponse,
    val historiskeIdenter: HistoriskeIdenterResponse,
    val identer: IdentResponse
) {

    companion object {
        private val logg = LoggerFactory.getLogger(this::class.java)
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")

        fun innhent(speedClient: SpeedClient, melding: Melding, callId: String): Personinformasjon {
            return runBlocking {
                val personinfoDeferred = async(Dispatchers.IO) {
                    sikkerlogg.info("henter personinfo for ${melding::class.simpleName} for {}", kv("fødselsnummer", melding.fødselsnummer()))
                    retry { speedClient.hentPersoninfo(melding.fødselsnummer(), callId) }
                }
                val historiskeIdenterDeferred = async(Dispatchers.IO) {
                    sikkerlogg.info("henter historiske identer for ${melding::class.simpleName} for {}", kv("fødselsnummer", melding.fødselsnummer()))
                    retry { speedClient.hentHistoriskeFødselsnumre(melding.fødselsnummer(), callId) }
                }
                val identerDeferred = async(Dispatchers.IO) {
                    sikkerlogg.info("henter aktørId for ${melding::class.simpleName} for {}", kv("fødselsnummer", melding.fødselsnummer()))
                    retry { speedClient.hentFødselsnummerOgAktørId(melding.fødselsnummer(), callId) }
                }

                Personinformasjon(personinfoDeferred.await(), historiskeIdenterDeferred.await(), identerDeferred.await())
            }
        }
    }
}