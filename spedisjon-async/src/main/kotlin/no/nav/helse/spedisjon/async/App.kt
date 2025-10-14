package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.azure.createAzureTokenClientFromEnvironment
import com.github.navikt.tbd_libs.kafka.AivenConfig
import com.github.navikt.tbd_libs.kafka.ConsumerProducerFactory
import com.github.navikt.tbd_libs.rapids_and_rivers_api.FailedMessage
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.OutgoingMessage
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection.MessageListener
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection.StatusListener
import com.github.navikt.tbd_libs.rapids_and_rivers_api.SentMessage
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.MeterRegistry
import java.net.http.HttpClient
import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    val env = System.getenv()

    val azure = createAzureTokenClientFromEnvironment(env)
    val objectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
    val speedClient = SpeedClient(HttpClient.newHttpClient(), objectMapper, azure)

    val httpMeldingtjeneste = HttpMeldingtjeneste(
        httpClient = HttpClient.newHttpClient(),
        tokenProvider = azure,
        objectMapper = objectMapper
    )


    val factory = ConsumerProducerFactory(AivenConfig.default)
    val rapidsConnection = RapidApplication.create(env, factory)

    val dataSourceBuilder = DataSourceBuilder(env)
    val ekspederingMediator = EkspederingMediator(EkspederingDao(dataSourceBuilder::dataSource), rapidsConnection)
    val meldingMediator = MeldingMediator(httpMeldingtjeneste, speedClient, ekspederingMediator)

    LogWrapper(rapidsConnection, meldingMediator).apply {
        NyeSøknader(this, meldingMediator)
        NyeFrilansSøknader(this, meldingMediator)
        NyeSelvstendigSøknader(this, meldingMediator)
        NyeArbeidsledigSøknader(this, meldingMediator)
        FremtidigSøknaderRiver(this, meldingMediator)
        FremtidigFrilansSøknaderRiver(this, meldingMediator)
        FremtidigSelvstendigSøknaderRiver(this, meldingMediator)
        FremtidigArbeidsledigSøknaderRiver(this, meldingMediator)
        SendteSøknaderArbeidsgiver(this, meldingMediator)
        SendteSøknaderNav(this, meldingMediator)
        SendteFrilansSøknader(this, meldingMediator)
        SendteSelvstendigSøknader(this, meldingMediator)
        SendteArbeidsledigSøknader(this, meldingMediator)
        AndreSøknaderRiver(this)
        ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Forespurte)
        ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Korrigerte)
        ArbeidsgiveropplysningerRiver(this, meldingMediator, Arbeidsgiveropplysninger.Selvbestemte)
        LpsOgAltinnInntektsmeldinger(this, meldingMediator)
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

    override fun publish(messages: List<OutgoingMessage>): Pair<List<SentMessage>, List<FailedMessage>> {
        messages.forEach {
            checkNotNull(it.key) {
                "Krever key for å sikre at vi publiserer meldinger med fnr som key"
            }
        }
        return rapidsConnection.publish(messages)
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
