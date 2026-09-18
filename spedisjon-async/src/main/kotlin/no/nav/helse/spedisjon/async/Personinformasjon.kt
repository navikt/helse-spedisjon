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
import java.util.*
import no.nav.sykepenger.libs.logging.loggInfo

data class Personinformasjon(
    val personinfo: PersonResponse,
    val historiskeIdenter: HistoriskeIdenterResponse,
    val identer: IdentResponse
) {

    companion object {
        fun innhent(speedClient: SpeedClient, melding: Melding, callId: String): Personinformasjon {
            return runBlocking {
                val personinfoDeferred = async(Dispatchers.IO) {
                    loggInfo(
                        "henter personinfo for ${melding::class.simpleName} (${melding.meldingsdetaljer.type})",
                        "fødselsnummer" to melding.meldingsdetaljer.fnr
                    )
                    retry {
                        when (val svar = speedClient.hentPersoninfo(melding.meldingsdetaljer.fnr, callId)) {
                            is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                            is Result.Ok -> svar.value
                        }
                    }
                }
                val historiskeIdenterDeferred = async(Dispatchers.IO) {
                    loggInfo(
                        "henter historiske identer for ${melding::class.simpleName} (${melding.meldingsdetaljer.type})",
                        "fødselsnummer" to melding.meldingsdetaljer.fnr
                    )
                    retry {
                        when (val svar = speedClient.hentHistoriskeFødselsnumre(melding.meldingsdetaljer.fnr, callId)) {
                            is Result.Error -> throw RuntimeException(svar.error, svar.cause)
                            is Result.Ok -> svar.value
                        }
                    }
                }
                val identerDeferred = async(Dispatchers.IO) {
                    loggInfo(
                        "henter aktørId for ${melding::class.simpleName} (${melding.meldingsdetaljer.type})",
                        "fødselsnummer" to melding.meldingsdetaljer.fnr
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
                loggInfo("beriker ${melding::class.simpleName}",
                    "jsonBody" to melding.meldingsdetaljer.jsonBody)
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

                    false -> loggInfo("Personen støttes ikke",
                        "aktørId" to identer.aktørId,
                        "jsonBody" to melding.meldingsdetaljer.jsonBody)
                }
            }
        }
    }
}
