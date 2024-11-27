package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.kafka.AivenConfig
import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection.MessageListener
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection.StatusListener
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.rapids_rivers.RapidApplication
import org.slf4j.LoggerFactory
import java.net.http.HttpClient

fun main() {
    val env = System.getenv()
    val erUtvikling = env["NAIS_CLUSTER_NAME"] == "dev-gcp"

    val azure = createAzureTokenClientFromEnvironment(env)
    val objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
    val speedClient = SpeedClient(HttpClient.newHttpClient(), objectMapper, azure)

    val dataSourceBuilder = DataSourceBuilder(env)
    val httpMeldingtjeneste = HttpMeldingtjeneste(
        httpClient = HttpClient.newHttpClient(),
        tokenProvider = azure,
        objectMapper = objectMapper
    )

    val inntektsmeldingDao = InntektsmeldingDao(httpMeldingtjeneste, dataSourceBuilder.dataSource)

    val factory = ConsumerProducerFactory(AivenConfig.default)

    val rapidsConnection = RapidApplication.create(env, factory)

    val ekspederingMediator = EkspederingMediator(EkspederingDao(dataSourceBuilder.dataSource), rapidsConnection)
    val meldingMediator = MeldingMediator(httpMeldingtjeneste, speedClient, ekspederingMediator)
    val inntektsmeldingTimeoutSekunder = env["KARANTENE_TID"]?.toLong() ?: 0L.also {
        val loggtekst = "KARANTENE_TID er tom; defaulter til ingen karantene"
        LoggerFactory.getLogger(::main.javaClass).info(loggtekst)
        LoggerFactory.getLogger("tjenestekall").info(loggtekst)
    }
    val inntektsmeldingMediator = InntektsmeldingMediator(
        speedClient,
        inntektsmeldingDao,
        ekspederingMediator,
        inntektsmeldingTimeoutSekunder = inntektsmeldingTimeoutSekunder
    )

    LogWrapper(rapidsConnection, meldingMediator).apply {
        NyeSøknader(this, meldingMediator)
        if (erUtvikling) NyeFrilansSøknader(this, meldingMediator)
        if (erUtvikling) NyeSelvstendigSøknader(this, meldingMediator)
        NyeArbeidsledigSøknader(this, meldingMediator)
        FremtidigSøknaderRiver(this, meldingMediator)
        if (erUtvikling) FremtidigFrilansSøknaderRiver(this, meldingMediator)
        if (erUtvikling) FremtidigSelvstendigSøknaderRiver(this, meldingMediator)
        FremtidigArbeidsledigSøknaderRiver(this, meldingMediator)
        SendteSøknaderArbeidsgiver(this, meldingMediator)
        SendteSøknaderNav(this, meldingMediator)
        if (erUtvikling) SendteFrilansSøknader(this, meldingMediator)
        if (erUtvikling) SendteSelvstendigSøknader(this, meldingMediator)
        SendteArbeidsledigSøknader(this, meldingMediator)
        AndreSøknaderRiver(this)
        Inntektsmeldinger(this, meldingMediator, inntektsmeldingMediator)
        Puls(this, inntektsmeldingMediator)
        AvbrutteSøknader(this, meldingMediator)
        AvbrutteArbeidsledigSøknader(this, meldingMediator)
    }.apply {
        register(object : StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}

internal class LogWrapper(
    private val rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
) : RapidsConnection(), MessageListener, StatusListener {

    init {
        rapidsConnection.register(this as MessageListener)
        rapidsConnection.register(this as StatusListener)
    }

    override fun onMessage(message: String, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        meldingMediator.beforeMessage()
        notifyMessage(message, this, metadata, meterRegistry)
        meldingMediator.afterMessage(message)
    }

    override fun publish(message: String) {
        throw IllegalStateException("Krever key for å sikre at vi publiserer meldinger med fnr som key")
    }

    override fun publish(key: String, message: String) {
        rapidsConnection.publish(key, message)
    }

    override fun rapidName() = "LogWrapper"

    override fun onNotReady(rapidsConnection: RapidsConnection) {
        notifyNotReady()
    }

    override fun onReady(rapidsConnection: RapidsConnection) {
        notifyReady()
    }

    override fun onShutdown(rapidsConnection: RapidsConnection) {
        notifyShutdown()
    }

    override fun onStartup(rapidsConnection: RapidsConnection) {
        notifyStartup()
    }

    override fun start() {
        rapidsConnection.start()
    }

    override fun stop() {
        rapidsConnection.stop()
    }
}