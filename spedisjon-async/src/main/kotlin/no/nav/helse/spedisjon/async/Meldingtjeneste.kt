package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.navikt.tbd_libs.azure.AzureTokenProvider
import com.github.navikt.tbd_libs.rapids_and_rivers.withMDC
import com.github.navikt.tbd_libs.result_object.Result
import com.github.navikt.tbd_libs.result_object.error
import com.github.navikt.tbd_libs.result_object.getOrThrow
import com.github.navikt.tbd_libs.result_object.map
import com.github.navikt.tbd_libs.result_object.ok
import com.github.navikt.tbd_libs.speed.Feilresponse
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import no.nav.sykepenger.libs.logging.loggInfo

interface Meldingtjeneste {
    fun nyMelding(meldingsdetaljer: NyMeldingRequest): NyMeldingResponse
    fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse
}

internal class HttpMeldingtjeneste(
    private val httpClient: HttpClient,
    private val tokenProvider: AzureTokenProvider,
    private val objectMapper: ObjectMapper,
    baseUrl: String? = null,
    scope: String? = null
) : Meldingtjeneste {
    private val baseUrl = baseUrl ?: "http://spedisjon"
    private val scope = scope ?: "api://${System.getenv("NAIS_CLUSTER_NAME")}.tbd.spedisjon/.default"

    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val callId = UUID.randomUUID().toString()
        return withMDC("callId" to callId) {
            val jsonInputString = objectMapper.writeValueAsString(request)
            loggInfo("legger melding til spedisjon", "melding" to request.toString())
            request("POST", "/api/melding", jsonInputString, callId)
                .map { response ->
                    when (response.statusCode()) {
                        200 -> convertResponseBody<NyMeldingOkResponse>(response).map {
                            NyMeldingResponse(internDokumentId = it.internDokumentId).ok()
                        }

                        409 -> convertResponseBody<NyMeldingOkResponse>(response).map {
                            NyMeldingResponse(internDokumentId = it.internDokumentId).ok()
                        }

                        else -> convertResponseBody<Feilresponse>(response).map {
                            Result.Error("Feil fra Spedisjon: ${it.detail}")
                        }
                    }
                }.getOrThrow()
        }
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        val callId = UUID.randomUUID().toString()
        val jsonInputString = objectMapper.writeValueAsString(HentMeldingerRequest(interneDokumentIder))
        return request("GET", "/api/meldinger", jsonInputString, callId)
            .map { response ->
                when (response.statusCode()) {
                    200 -> convertResponseBody<HentMeldingerOkResponse>(response).map {
                        HentMeldingerResponse(
                            meldinger = it.meldinger.map { dto ->
                                MeldingDto(
                                    type = dto.type,
                                    fnr = dto.fnr,
                                    internDokumentId = dto.internDokumentId,
                                    eksternDokumentId = dto.eksternDokumentId,
                                    duplikatkontroll = dto.duplikatkontroll,
                                    jsonBody = dto.jsonBody
                                )
                            }
                        ).ok()
                    }

                    else -> convertResponseBody<Feilresponse>(response).map {
                        Result.Error("Feil fra Spedisjon: ${it.detail}")
                    }
                }
            }.getOrThrow()
    }

    private fun request(
        method: String,
        action: String,
        jsonInputString: String,
        callId: String
    ): Result<HttpResponse<String>> {
        return tokenProvider.bearerToken(scope).map { token ->
            try {
                val request = HttpRequest.newBuilder()
                    .uri(URI("$baseUrl$action"))
                    .timeout(Duration.ofSeconds(60))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer ${token.token}")
                    .header("callId", callId)
                    .method(method, HttpRequest.BodyPublishers.ofString(jsonInputString))
                    .build()

                httpClient.send(request, HttpResponse.BodyHandlers.ofString()).ok()
            } catch (err: Exception) {
                "Feil ved sending av request: ${err.message}".error(err)
            }
        }
    }

    private inline fun <reified T> convertResponseBody(response: HttpResponse<String>): Result<T> {
        if (response.body().isNullOrBlank()) {
            return "Fikk tomt svar fra Spedisjon (status=${response.statusCode()})".error()
        }
        return try {
            objectMapper.readValue<T>(response.body()).ok()
        } catch (err: Exception) {
            val feilmelding = "Klarte ikke å tolke svar fra Spedisjon (status=${response.statusCode()}): ${err.message}"
            err.error(feilmelding)
        }
    }

    private data class HentMeldingerRequest(val internDokumentIder: List<UUID>)
    private data class NyMeldingOkResponse(val internDokumentId: UUID)
    private data class HentMeldingerOkResponse(val meldinger: List<MeldingResponse>)
    private data class MeldingResponse(
        val type: String,
        val fnr: String,
        val internDokumentId: UUID,
        val eksternDokumentId: UUID,
        val duplikatkontroll: String,
        val jsonBody: String
    )
}

data class NyMeldingResponse(val internDokumentId: UUID)

data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)

class HentMeldingerResponse(
    val meldinger: List<MeldingDto>
)

data class MeldingDto(
    val type: String,
    val fnr: String,
    val internDokumentId: UUID,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)
