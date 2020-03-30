package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.util.*

internal class AktørregisteretClient(
    private val baseUrl: String,
    private val stsRestClient: StsRestClient
) {

    companion object {
        private val objectMapper = jacksonObjectMapper()
        private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
    }

    internal fun hentFødselsnummer(aktørId: String) =
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
        tjenestekallLog.info("svar fra aktørregisteret: responseCode=$responseCode responseBody=$responseBody callId=$callId")
        if (responseCode >= 300 || responseBody == null) return emptyList()
        return objectMapper.readTree(responseBody).path("identer")
            .takeIf { it.isArray }
            ?.filter { it.hasNonNull("identgruppe") && it.hasNonNull("ident") }
            ?.filter { IdentType.values().map(Enum<*>::name).contains(it["identgruppe"].asText()) }
            ?.map { IdentType.valueOf(it.path("identgruppe").asText()) to it.path("ident").asText() }
            ?: emptyList()
    }

    private enum class IdentType {
        AktoerId, NorskIdent
    }
}
