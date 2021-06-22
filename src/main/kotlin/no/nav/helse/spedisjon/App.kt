package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.spedisjon.AktørregisteretClient.AktørregisteretRestClient
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.File
import java.util.*

private val log = LoggerFactory.getLogger("no.nav.helse.spedisjon.App")

fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)
    val meldingDao = MeldingDao(dataSourceBuilder.getDataSource())

    val aktørregisteretClient = AktørregisteretRestClient(env.getValue("AKTORREGISTERET_URL"), StsRestClient(
        baseUrl = "http://security-token-service.default.svc.nais.local",
        username = "/var/run/secrets/nais.io/service_user/username".readFile(),
        password = "/var/run/secrets/nais.io/service_user/password".readFile()
    ))

    val meldingMediator = MeldingMediator(meldingDao, aktørregisteretClient, env["STREAM_TO_RAPID"]?.let { "false" != it.toLowerCase() } ?: true)

    LogWrapper(RapidApplication.create(env), meldingMediator).apply {
        NyeSøknader(this, meldingMediator)
        FremtidigSøknaderRiver(this, meldingMediator)
        SendteSøknaderArbeidsgiver(this, meldingMediator)
        SendteSøknaderNav(this, meldingMediator)
        Inntektsmeldinger(this, meldingMediator)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}

internal class LogWrapper(
    private val rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
) : RapidsConnection(), RapidsConnection.MessageListener {

    init {
        rapidsConnection.register(this)
    }

    override fun onMessage(message: String, context: MessageContext) {
        meldingMediator.beforeMessage(message)
        notifyMessage(message, this)
        meldingMediator.afterMessage(message)
    }

    override fun publish(message: String) {
        throw IllegalStateException("Krever key for å sikre at vi publiserer meldinger med fnr som key")
    }

    override fun publish(key: String, message: String) {
        rapidsConnection.publish(key, message)
    }

    private fun checkFatalError(metadata: RecordMetadata?, err: Exception?) {
        if (err == null || err !is AuthorizationException) return
        log.error("Stopping rapid due to fatal error: ${err.message}", err)
        stop()
    }

    override fun start() {
        rapidsConnection.start()
    }

    override fun stop() {
        rapidsConnection.stop()
    }
}

private fun String.readFile() = File(this).readText(Charsets.UTF_8)
