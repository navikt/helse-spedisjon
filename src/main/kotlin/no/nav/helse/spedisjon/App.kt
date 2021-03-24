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

    val aivenProducer = createAivenProducer(env)

    val meldingMediator = MeldingMediator(meldingDao, aktørregisteretClient, env["STREAM_TO_RAPID"]?.let { "false" != it.toLowerCase() } ?: true)

    LogWrapper(RapidApplication.create(env), meldingMediator, env["KAFKA_RAPID_TOPIC_AIVEN"], aivenProducer).apply {
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

private fun createAivenProducer(env: Map<String, String>): KafkaProducer<String, String>? {
    if (!env.containsKey("KAFKA_RAPID_TOPIC_AIVEN")) return null.also { log.info("Not creating aiven producer") }
    log.info("Creating aiven producer")
    val properties = Properties().apply {
        put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, env.getValue("KAFKA_BROKERS"))
        put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name)
        put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "")
        put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "jks")
        put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
        put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, env.getValue("KAFKA_TRUSTSTORE_PATH"))
        put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))
        put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, env.getValue("KAFKA_KEYSTORE_PATH"))
        put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, env.getValue("KAFKA_CREDSTORE_PASSWORD"))

        put(ProducerConfig.ACKS_CONFIG, "1")
        put(ProducerConfig.LINGER_MS_CONFIG, "0")
        put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    }
    return KafkaProducer(properties, StringSerializer(), StringSerializer())
}

internal class LogWrapper(
    private val rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
    private val aivenTopic: String? = null,
    private val aivenProducer: KafkaProducer<String, String>? = null
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
        aivenProducer?.send(ProducerRecord(aivenTopic, key, message), ::checkFatalError)?.also {
            log.info("producing message (with key) to aiven")
        } ?: rapidsConnection.publish(key, message)
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
