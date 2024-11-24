package no.nav.helse.spedisjon.api

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.naisful.naisApp
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.request.*
import io.ktor.server.routing.*
import io.micrometer.core.instrument.Clock
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import io.prometheus.metrics.model.registry.PrometheusRegistry
import no.nav.helse.spedisjon.api.tjeneste.LokalMeldingtjeneste
import org.slf4j.LoggerFactory
import java.net.URI

private val logg = LoggerFactory.getLogger(::main.javaClass)
private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
private val objectMapper = jacksonObjectMapper()
    .registerModule(JavaTimeModule())
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    .enable(SerializationFeature.INDENT_OUTPUT)

fun main() {
    Thread.currentThread().setUncaughtExceptionHandler { _, e ->
        logg.error("Ufanget exception: {}", e.message, e)
        sikkerlogg.error("Ufanget exception: {}", e.message, e)
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
        )
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
