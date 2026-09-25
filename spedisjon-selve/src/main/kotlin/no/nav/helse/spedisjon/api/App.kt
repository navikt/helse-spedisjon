package no.nav.helse.spedisjon.api

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.naisful.defaultStatusPagesConfig
import com.github.navikt.tbd_libs.naisful.naisApp
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.plugins.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.cio.ChannelReadException
import io.ktor.utils.io.ClosedReadChannelException
import io.micrometer.core.instrument.Clock
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import no.nav.helse.spedisjon.api.tjeneste.LokalMeldingtjeneste
import no.nav.sykepenger.libs.logging.navngittLogger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import kotlinx.coroutines.CancellationException

private val logg = LoggerFactory.getLogger(::main.javaClass)
private val logger = navngittLogger("no.nav.helse.spedisjon.api.App")
private val objectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .enable(SerializationFeature.INDENT_OUTPUT)

fun main() {
    Thread.currentThread().setUncaughtExceptionHandler { _, e ->
        logger.error("Ufanget exception", e)
    }
    launchApp(System.getenv())
}

private fun launchApp(env: Map<String, String>) {
    val erUtvikling = env["NAIS_CLUSTER_NAME"] == "dev-gcp"

    val azureApp = AzureApp(
        jwkProvider = JwkProviderBuilder(URI(env.getValue("AZURE_OPENID_CONFIG_JWKS_URI")).toURL()).build(),
        issuer = env.getValue("AZURE_OPENID_CONFIG_ISSUER"),
        clientId = env.getValue("AZURE_APP_CLIENT_ID"),
    )

    val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, PrometheusRegistry.defaultRegistry, Clock.SYSTEM)

    val dataSourceBuilder = DataSourceBuilder(meterRegistry)
    val lokalMeldingtjeneste = LokalMeldingtjeneste(MeldingDao(dataSourceBuilder.dataSource))

    val app = naisApp(
        meterRegistry = meterRegistry,
        objectMapper = objectMapper,
        applicationLogger = logg,
        callLogger = LoggerFactory.getLogger("no.nav.helse.spedisjon.api.CallLogging"),
        timersConfig = { call, _ ->
            this
                .tag("azp_name", call.principal<JWTPrincipal>()?.get("azp_name") ?: "n/a")
                .tag("konsument", call.request.header("L5d-Client-Id") ?: "n/a")
        },
        mdcEntries = mapOf(
            "azp_name" to { call: ApplicationCall -> call.principal<JWTPrincipal>()?.get("azp_name") },
            "konsument" to { call: ApplicationCall -> call.request.header("L5d-Client-Id") }
        ),
        statusPagesConfig = { spedisjonStatusPages() }
    ) {
        monitor.subscribe(ApplicationStarted) {
            dataSourceBuilder.migrate()
        }
        authentication { azureApp.konfigurerJwtAuth(this) }
        routing {
            authenticate {
                api(lokalMeldingtjeneste)
            }
        }
    }
    app.start(wait = true)
}

internal fun StatusPagesConfig.spedisjonStatusPages() {
    defaultStatusPagesConfig()
    exception<BadRequestException> { call, cause ->
        val status = if (cause.skyldesAvbruttKanal()) {
            logger.warn("Midlertidig feil ved lesing av request body", cause)
            HttpStatusCode.ServiceUnavailable
        } else {
            HttpStatusCode.BadRequest
        }
        call.response.header(HttpHeaders.ContentType, ContentType.Application.ProblemJson.toString())
        call.respond(
            status,
            SpedisjonFeilresponse(
                type = if (status == HttpStatusCode.ServiceUnavailable) {
                    "urn:error:temporary"
                } else {
                    "urn:error:bad_request"
                },
                title = status.description,
                status = status.value,
                detail = if (status == HttpStatusCode.ServiceUnavailable) {
                    "Spedisjon-API er utilgjengelig: ${cause.message}"
                } else {
                    cause.message
                },
                instance = call.request.uri,
                callId = call.callId
            )
        )
    }
}

internal fun Throwable.skyldesAvbruttKanal(): Boolean {
    val årsaker = generateSequence(this) { it.cause }.toList()
    return årsaker.none { it is JsonProcessingException } &&
        årsaker.any {
            it is IOException || it is CancellationException
        }
}
