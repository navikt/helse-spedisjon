package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.navikt.tbd_libs.azure.AzureTokenProvider
import com.github.navikt.tbd_libs.result_object.*
import com.github.navikt.tbd_libs.speed.Feilresponse
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalDateTime
import java.util.*

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
        val jsonInputString = objectMapper.writeValueAsString(request)
        return request("POST", "/api/melding", jsonInputString, callId)
            .map {  response ->
                when (response.statusCode()) {
                    200 -> convertResponseBody<NyMeldingOkResponse>(response).map {
                        NyMeldingResponse.OK(internDokumentId = it.internDokumentId).ok()
                    }
                    409 -> NyMeldingResponse.Duplikatkontroll.ok()
                    else -> convertResponseBody<Feilresponse>(response).map {
                        Result.Error("Feil fra Spedisjon: ${it.detail}")
                    }
                }
            }.getOrThrow()
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        val callId = UUID.randomUUID().toString()
        val jsonInputString = objectMapper.writeValueAsString(HentMeldingerRequest(interneDokumentIder))
        return request("GET", "/api/meldinger", jsonInputString, callId)
            .map {  response ->
                when (response.statusCode()) {
                    200 -> convertResponseBody<HentMeldingerOkResponse>(response).map {
                        HentMeldingerResponse(
                            meldinger = it.meldinger.map { dto ->
                                MeldingDto(
                                    type = dto.type,
                                    fnr = dto.fnr,
                                    internDokumentId = dto.internDokumentId,
                                    eksternDokumentId = dto.eksternDokumentId,
                                    rapportertDato = dto.rapportertDato,
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

    private fun request(method: String, action: String, jsonInputString: String, callId: String): Result<HttpResponse<String>> {
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
        return try {
            objectMapper.readValue<T>(response.body()).ok()
        } catch (err: Exception) {
            err.error(err.message ?: "JSON parsing error")
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
        val rapportertDato: LocalDateTime,
        val duplikatkontroll: String,
        val jsonBody: String
    )
}

internal class LokalMeldingtjeneste(private val dao: MeldingDao) : Meldingtjeneste {
    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val dto = NyMeldingDto(
            type = request.type,
            fnr = request.fnr,
            eksternDokumentId = request.eksternDokumentId,
            rapportertDato = request.rapportertDato,
            duplikatkontroll = request.duplikatkontroll,
            jsonBody = request.jsonBody
        )
        val internId = dao.leggInn(dto) ?: return NyMeldingResponse.Duplikatkontroll
        return NyMeldingResponse.OK(internId)
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        return HentMeldingerResponse(
            meldinger = dao.hentMeldinger(interneDokumentIder)
        )
    }
}

sealed interface NyMeldingResponse {
    data object Duplikatkontroll : NyMeldingResponse
    data class OK(val internDokumentId: UUID) : NyMeldingResponse
}

data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)

class HentMeldingerResponse(
    val meldinger: List<MeldingDto>
)