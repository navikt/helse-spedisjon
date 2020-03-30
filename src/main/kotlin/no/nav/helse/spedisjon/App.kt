package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.spedisjon.AktørregisteretClient.AktørregisteretRestClient
import no.nav.helse.spedisjon.AktørregisteretClient.CachedAktørregisteretClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File

fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)
    val meldingDao = MeldingDao(dataSourceBuilder.getDataSource())

    val aktørregisteretClient = CachedAktørregisteretClient(AktørregisteretRestClient(env.getValue("AKTORREGISTERET_URL"), StsRestClient(
        baseUrl = "http://security-token-service.default.svc.nais.local",
        username = "/var/run/secrets/nais.io/service_user/username".readFile(),
        password = "/var/run/secrets/nais.io/service_user/password".readFile()
    )))

    LogWrapper(RapidApplication.create(env) { _, rapid -> rapid.seekToBeginning() }, LoggerFactory.getLogger("tjenestekall")).apply {
        NyeSøknader(this, meldingDao, problemsCollector, aktørregisteretClient)
        SendteSøknaderArbeidsgiver(this, meldingDao, problemsCollector, aktørregisteretClient)
        SendteSøknaderNav(this, meldingDao, problemsCollector, aktørregisteretClient)
        Inntektsmeldinger(this, meldingDao, problemsCollector, aktørregisteretClient)
        AndreHendelser(this, problemsCollector)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}

internal interface ProblemsCollector {
    fun add(type: String, problems: MessageProblems)
}

internal class LogWrapper(
    private val rapidsConnection: RapidsConnection,
    private val logger: Logger
) : RapidsConnection(), RapidsConnection.MessageListener {
    internal val problemsCollector = Collector

    init {
        rapidsConnection.register(this)
    }

    override fun onMessage(message: String, context: MessageContext) {
        listeners.forEach { it.onMessage(message, context) }
        logProblems(message)
        problemsCollector.messageProblems.clear()
    }

    private fun logProblems(message: String) {
        if (problemsCollector.messageProblems.size != listeners.size) return
        logger.info(
            "Kunne ikke forstå melding:\n{}\n\nProblemer:\n{}",
            message,
            problemsCollector.messageProblems.joinToString(separator = "\n\n") {
                "${it.first}:\n${it.second}"
            })
    }

    override fun publish(message: String) {
        rapidsConnection.publish(message)
    }

    override fun publish(key: String, message: String) {
        rapidsConnection.publish(key, message)
    }

    override fun start() {
        rapidsConnection.start()
    }

    override fun stop() {
        rapidsConnection.stop()
    }

    internal companion object Collector : ProblemsCollector {
        private val messageProblems = mutableListOf<Pair<String, MessageProblems>>()

        override fun add(type: String, problems: MessageProblems) {
            messageProblems.add(type to problems)
        }
    }
}

private fun String.readFile() = File(this).readText(Charsets.UTF_8)
