package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.spedisjon.AktørregisteretClient.AktørregisteretRestClient
import java.io.File

fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)
    val meldingDao = MeldingDao(dataSourceBuilder.getDataSource())

    val aktørregisteretClient = AktørregisteretRestClient(env.getValue("AKTORREGISTERET_URL"), StsRestClient(
        baseUrl = "http://security-token-service.default.svc.nais.local",
        username = "/var/run/secrets/nais.io/service_user/username".readFile(),
        password = "/var/run/secrets/nais.io/service_user/password".readFile()
    ))

    val meldingMediator = MeldingMediator(meldingDao, aktørregisteretClient)

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
) : RapidsConnection(), RapidsConnection.MessageListener, RapidsConnection.StatusListener {

    init {
        rapidsConnection.register(this as MessageListener)
        rapidsConnection.register(this as StatusListener)
    }

    override fun onMessage(message: String, context: MessageContext) {
        meldingMediator.beforeMessage()
        notifyMessage(message, this)
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

private fun String.readFile() = File(this).readText(Charsets.UTF_8)
