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
import com.github.navikt.tbd_libs.retry.PredefinerteUtsettelser
import com.github.navikt.tbd_libs.retry.retryBlocking
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import no.nav.sykepenger.libs.logging.loggInfo
import no.nav.sykepenger.libs.logging.loggWarn

interface Meldingtjeneste {
    fun nyMelding(request: NyMeldingRequest): NyMeldingResponse

    fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse
}

internal class HttpMeldingtjeneste(
    private val httpClient: HttpClient,
    private val tokenProvider: AzureTokenProvider,
    private val objectMapper: ObjectMapper,
    baseUrl: String? = null,
    scope: String? = null,
    private val retryUtsettelser: () -> Iterator<Duration> = {
        PredefinerteUtsettelser(
            Duration.ofMillis(200),
            Duration.ofMillis(600),
            Duration.ofMillis(1200)
        )
    }
) : Meldingtjeneste {
    private val baseUrl = baseUrl ?: "http://spedisjon"
    private val scope = scope ?: "api://${System.getenv("NAIS_CLUSTER_NAME")}.tbd.spedisjon/.default"

    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val callId = UUID.randomUUID().toString()
        return withMDC("callId" to callId) {
            val jsonInputString = objectMapper.writeValueAsString(request)
            loggInfo("sender melding til spedisjon", "melding" to request.toString())
            retryBlocking(
                utsettelser = retryUtsettelser(),
                avbryt = { it !is RetryableSpedisjonException }
            ) {
                request("POST", "/api/melding", jsonInputString, callId)
                    .map { response ->
                        when (response.statusCode()) {
                            200, 201 ->
                                convertResponseBody<NyMeldingOkResponse>(response).map {
                                    NyMeldingResponse(internDokumentId = it.internDokumentId).ok()
                                }

                            429, in 502..504 -> throw RetryableSpedisjonException(
                                "Midlertidig feil fra APIet (status=${response.statusCode()})"
                            )

                            else -> feilFraSpedisjon(response)
                        }
                    }.getOrThrow()
            }
        }
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        val callId = UUID.randomUUID().toString()
        val jsonInputString = objectMapper.writeValueAsString(HentMeldingerRequest(interneDokumentIder))
        return request("GET", "/api/meldinger", jsonInputString, callId)
            .map { response ->
                when (response.statusCode()) {
                    200 ->
                        convertResponseBody<HentMeldingerOkResponse>(response).map {
                            HentMeldingerResponse(
                                meldinger =
                                    it.meldinger.map { dto ->
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

                    else -> feilFraSpedisjon(response)
                }
            }.getOrThrow()
    }

    private fun request(
        method: String,
        action: String,
        jsonInputString: String,
        callId: String
    ): Result<HttpResponse<String>> =
        tokenProvider.bearerToken(scope).map { token ->
            try {
                val request =
                    HttpRequest
                        .newBuilder()
                        .uri(URI("$baseUrl$action"))
                        .timeout(Duration.ofSeconds(60))
                        .header("Accept", "application/json")
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer ${token.token}")
                        .header("callId", callId)
                        .method(method, HttpRequest.BodyPublishers.ofString(jsonInputString))
                        .build()

                httpClient
                    .send(request, HttpResponse.BodyHandlers.ofString())
                    .also { response ->
                        if (response.statusCode() !in 200..299) {
                            loggWarn(
                                "HTTP-feil fra Spedisjon",
                                "http_status" to response.statusCode().toString(),
                                "connection_close" to
                                    (
                                        response
                                            .headers()
                                            .firstValue("Connection")
                                            .orElse(null)
                                            ?.equals("close", ignoreCase = true) == true
                                    ).toString(),
                                "content_type" to
                                    when (
                                        response
                                            .headers()
                                            .firstValue("Content-Type")
                                            .orElse(null)
                                            ?.substringBefore(';')
                                            ?.trim()
                                            ?.lowercase()
                                    ) {
                                        "text/plain" -> "text/plain"
                                        "application/json" -> "application/json"
                                        "application/problem+json" -> "application/problem+json"
                                        null -> "mangler"
                                        else -> "annet"
                                    }
                            )
                        }
                    }.ok()
                // Midlertidige nettverksfeil kan oppstå uten HTTP-svar og må derfor prøves på nytt.
            } catch (err: IOException) {
                loggWarn("Nettverksfeil ved kall til Spedisjon", err)
                throw RetryableSpedisjonException(
                    "Midlertidig feil ved sending av request til Spedisjon",
                    err
                )
            } catch (err: Exception) {
                "Feil ved sending av request: ${err.message}".error(err)
            }
        }

    // Selv om spedisjon sin StatusPages-håndtering svarer med Feilresponse-JSON ved app-feil,
    // kan infrastruktur foran spedisjon (f.eks. service mesh) svare med ren tekst, for eksempel
    // ved tidsavbrudd eller at tilkoblingen ikke lenger fungerer.
    private fun <T> feilFraSpedisjon(response: HttpResponse<String>): Result<T> {
        val body =
            response.body().takeIf { it.isNotBlank() }
                ?: run {
                    loggWarn("Feilrespons uten innhold fra Spedisjon", "http_status" to response.statusCode().toString())
                    return Result.Error("Feil fra Spedisjon (status=${response.statusCode()}, response body er tom)")
                }

        return try {
            objectMapper.readValue<SpedisjonFeilresponse>(body).let { feilresponse ->
                val problemType =
                    when (feilresponse.type.toString()) {
                        "urn:error:bad_request", "urn:error:temporary", "urn:error:internal_error" ->
                            feilresponse.type.toString()
                        else -> "annet"
                    }
                loggWarn(
                    "Feilsvar fra Spedisjon i problemformat",
                    "http_status" to response.statusCode().toString(),
                    "body_status" to feilresponse.status.toString(),
                    "problem_type" to problemType
                )
                Result.Error("Feil fra Spedisjon (status=${response.statusCode()}): ${feilresponse.detail}")
            }
        } catch (_: Exception) {
            loggWarn("Feilrespons  fra Spedisjon uten kjent problemformat", "http_status" to response.statusCode().toString())
            val tekst = body.takeIf { it.isNotBlank() }?.take(500) ?: "(tomt svar)"
            Result.Error("Feil fra Spedisjon (status=${response.statusCode()}): $tekst")
        }
    }

    private inline fun <reified T> convertResponseBody(response: HttpResponse<String>): Result<T> {
        if (response.body().isNullOrBlank()) {
            return "Fikk tomt svar fra Spedisjon (status=${response.statusCode()})".error()
        }
        return try {
            objectMapper.readValue<T>(response.body()).ok()
        } catch (err: Exception) {
            val feilmelding = "Klarte ikke å mappe svar fra Spedisjon til ${T::class} (status=${response.statusCode()})"
            err.error(feilmelding)
        }
    }

    private data class HentMeldingerRequest(
        val internDokumentIder: List<UUID>
    )

    private data class NyMeldingOkResponse(
        val internDokumentId: UUID
    )

    private data class HentMeldingerOkResponse(
        val meldinger: List<MeldingResponse>
    )

    private data class MeldingResponse(
        val type: String,
        val fnr: String,
        val internDokumentId: UUID,
        val eksternDokumentId: UUID,
        val duplikatkontroll: String,
        val jsonBody: String
    )

    private data class SpedisjonFeilresponse(
        val type: URI,
        val title: String,
        val status: Int,
        val detail: String?,
        val instance: URI? = null,
        val callId: String? = null
    )

    private class RetryableSpedisjonException(
        message: String,
        cause: Throwable? = null
    ) : RuntimeException(message, cause)
}

data class NyMeldingResponse(
    val internDokumentId: UUID
)

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
