package no.nav.helse.spedisjon

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.*

internal interface AktørregisteretClient {
    fun hentFødselsnummer(aktørId: String): String?

   class AktørregisteretRestClient(
        private val baseUrl: String,
        private val stsRestClient: StsRestClient
    ) : AktørregisteretClient {

        companion object {
            private val objectMapper = jacksonObjectMapper()
            private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
            private val oppslagCounter = Counter.build("aktorregisteret_oppslag_totals", "Antall oppslag gjort mot aktørregisteret")
                .labelNames("http_status")
                .register()
        }

        override fun hentFødselsnummer(aktørId: String) =
            hentIdenter(aktørId).firstOrNull { it.first == IdentType.NorskIdent }?.second

        private fun hentIdenter(personident: String): List<Pair<IdentType, String>> {
            val callId = UUID.randomUUID().toString()
            val (responseCode, responseBody) = with(URL("$baseUrl/api/v1/identer?gjeldende=true").openConnection() as HttpURLConnection) {
                requestMethod = "GET"
                setRequestProperty("Authorization", "Bearer ${stsRestClient.token()}")
                setRequestProperty("Accept", "application/json")
                setRequestProperty("Nav-Call-Id", callId)
                setRequestProperty("Nav-Consumer-Id", "spedisjon")
                setRequestProperty("Nav-Personidenter", personident)
                val stream: InputStream? = if (responseCode < 300) this.inputStream else this.errorStream
                responseCode to stream?.bufferedReader()?.readText().also {
                    disconnect()
                }
            }
            oppslagCounter.labels("$responseCode").inc()
            tjenestekallLog.info("svar fra aktørregisteret: responseCode=$responseCode responseBody=$responseBody callId=$callId")
            if (responseCode >= 300 || responseBody == null) return emptyList()
            return try {
                val json = objectMapper.readTree(responseBody)
                    .path(personident)

                if (json.path("feilmelding").isTextual) {
                    tjenestekallLog.info(
                        "feilmelding fra aktørregisteret: {} callId=$callId",
                        json.path("feilmelding").asText()
                    )
                    emptyList()
                } else {
                    json.path("identer")
                        .takeIf { it.isArray }
                        ?.filter { it.hasNonNull("identgruppe") && it.hasNonNull("ident") && it.hasNonNull("gjeldende") }
                        ?.filter { it["gjeldende"].asBoolean() }
                        ?.filter { IdentType.values().map(Enum<*>::name).contains(it["identgruppe"].asText()) }
                        ?.map { IdentType.valueOf(it.path("identgruppe").asText()) to it.path("ident").asText() }
                        ?: emptyList()
                }
            } catch (err: JsonParseException) {
                tjenestekallLog.info("ugyldig json responseBody=$responseBody callId=$callId: {}", err.message, err)
                emptyList()
            }
        }

        private enum class IdentType {
            AktoerId, NorskIdent
        }
    }
}
