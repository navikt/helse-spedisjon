package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.withMDC
import com.github.navikt.tbd_libs.result_object.Result
import com.github.navikt.tbd_libs.retry.retry
import com.github.navikt.tbd_libs.speed.HistoriskeIdenterResponse
import com.github.navikt.tbd_libs.speed.IdentResponse
import com.github.navikt.tbd_libs.speed.PersonResponse
import com.github.navikt.tbd_libs.speed.PersonResponse.Adressebeskyttelse
import com.github.navikt.tbd_libs.speed.SpeedClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import java.util.*

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
                    sikkerlogg.info(
                        "henter personinfo for ${melding::class.simpleName} (${melding.meldingsdetaljer.type}) for {}",
                        kv("fødselsnummer", melding.meldingsdetaljer.fnr)
                    )
                    retry {
                        when (val svar = speedClient.hentPersoninfo(melding.meldingsdetaljer.fnr, callId)) {
                            is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                            is Result.Ok -> svar.value
                        }
                    }
                }
                val historiskeIdenterDeferred = async(Dispatchers.IO) {
                    sikkerlogg.info(
                        "henter historiske identer for ${melding::class.simpleName} (${melding.meldingsdetaljer.type}) for {}",
                        kv("fødselsnummer", melding.meldingsdetaljer.fnr)
                    )
                    retry {
                        when (val svar = speedClient.hentHistoriskeFødselsnumre(melding.meldingsdetaljer.fnr, callId)) {
                            is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                            is Result.Ok -> svar.value
                        }
                    }
                }
                val identerDeferred = async(Dispatchers.IO) {
                    sikkerlogg.info(
                        "henter aktørId for ${melding::class.simpleName} (${melding.meldingsdetaljer.type}) for {}",
                        kv("fødselsnummer", melding.meldingsdetaljer.fnr)
                    )
                    retry {
                        when (val svar = speedClient.hentFødselsnummerOgAktørId(melding.meldingsdetaljer.fnr, callId)) {
                            is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                            is Result.Ok -> svar.value
                        }
                    }
                }

                Personinformasjon(
                    personinfoDeferred.await(),
                    historiskeIdenterDeferred.await(),
                    identerDeferred.await()
                )
            }
        }

        fun berikMeldingOgBehandleDen(speedClient: SpeedClient, melding: Melding, håndtering: (Berikelse) -> Unit) {
            val callId = UUID.randomUUID().toString()
            withMDC(
                mapOf(
                    "callId" to callId,
                    "ekstern_dokument_id" to "${melding.meldingsdetaljer.eksternDokumentId}",
                    "intern_dokument_id" to "${melding.internId}"
                )
            ) {
                sikkerlogg.info("beriker ${melding::class.simpleName}:\n${melding.meldingsdetaljer.jsonBody}")
                val (personinfo, historiskeIdenter, identer) = innhent(speedClient, melding, callId)
                val støttes = personinfo.adressebeskyttelse !in setOf(
                    Adressebeskyttelse.STRENGT_FORTROLIG,
                    Adressebeskyttelse.STRENGT_FORTROLIG_UTLAND
                )
                when (støttes) {
                    true -> {
                        val berikelse = Berikelse(
                            fødselsdato = personinfo.fødselsdato,
                            dødsdato = personinfo.dødsdato,
                            aktørId = identer.aktørId,
                            historiskeFolkeregisteridenter = historiskeIdenter.fødselsnumre
                        )
                        håndtering(berikelse)
                    }

                    false -> sikkerlogg.info("Personen støttes ikke ${identer.aktørId}:\n${melding.meldingsdetaljer.jsonBody}")
                }
            }
        }
    }
}