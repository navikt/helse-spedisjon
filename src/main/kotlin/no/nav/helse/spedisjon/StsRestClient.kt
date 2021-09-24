package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.URL
import java.time.LocalDateTime
import java.util.*

internal class StsRestClient(private val baseUrl: String, username: String, password: String) {
    private companion object {
        private val objectMapper = jacksonObjectMapper()
        private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
        private val base64Encoder = Base64.getEncoder()
    }

    private val authorization = base64Encoder.encodeToString("$username:$password".toByteArray())
    private var cachedOidcToken: Token? = null

    fun token(): String {
        if (Token.shouldRenew(cachedOidcToken)) cachedOidcToken = fetchToken()
        return cachedOidcToken!!.accessToken
    }

    private fun fetchToken(): Token {
        val (responseCode, responseBody) = with(URL("$baseUrl/rest/v1/sts/token?grant_type=client_credentials&scope=openid").openConnection() as HttpURLConnection) {
            requestMethod = "GET"
            setRequestProperty("Authorization", "Basic $authorization")
            setRequestProperty("Accept", "application/json")
            val stream: InputStream? = if (responseCode < 300) this.inputStream else this.errorStream
            responseCode to stream?.bufferedReader()?.readText().also {
                disconnect()
            }
        }
        if (responseCode >= 300 || responseBody == null) throw RuntimeException("failed to fetch token from sts")
        return objectMapper.readTree(responseBody).let {
            Token(
                accessToken = it.path("access_token").asText(),
                type = it.path("token_type").asText(),
                expiresIn = it.path("expires_in").asInt()
            )
        }
    }

    private class Token(val accessToken: String, private val type: String, private val expiresIn: Int) {
        // expire 10 seconds before actual expiry. for great margins.
        private val expirationTime: LocalDateTime = LocalDateTime.now().plusSeconds(expiresIn - 10L)

        companion object {
            fun shouldRenew(token: Token?): Boolean {
                if (token == null) {
                    return true
                }

                return isExpired(token)
            }

            private fun isExpired(token: Token): Boolean {
                return token.expirationTime.isBefore(LocalDateTime.now())
            }
        }
    }
}
