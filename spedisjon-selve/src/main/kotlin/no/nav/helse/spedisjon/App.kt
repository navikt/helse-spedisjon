package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection
import org.slf4j.LoggerFactory

fun main() {
    val env = System.getenv()
    val erUtvikling = env["NAIS_CLUSTER_NAME"] == "dev-gcp"
    val dataSourceBuilder = DataSourceBuilder(env)
    val dataSource = dataSourceBuilder.getDataSource()
    val meldingDao = MeldingDao(dataSource)
    val inntektsmeldingDao = InntektsmeldingDao(dataSource)
    val berikelseDao = BerikelseDao(dataSource)

    val meldingMediator = MeldingMediator(meldingDao, berikelseDao)
    val personBerikerMediator = PersonBerikerMediator(meldingDao, berikelseDao, meldingMediator)
    val inntektsmeldingTimeoutSekunder = env["KARANTENE_TID"]?.toLong() ?: 0L.also {
        val loggtekst = "KARANTENE_TID er tom; defaulter til ingen karantene"
        LoggerFactory.getLogger(::main.javaClass).info(loggtekst)
        LoggerFactory.getLogger("tjenestekall").info(loggtekst)
    }
    val inntektsmeldingMediator = InntektsmeldingMediator(
        dataSource,
        meldingDao,
        inntektsmeldingDao,
        berikelseDao,
        meldingMediator,
        inntektsmeldingTimeoutSekunder = inntektsmeldingTimeoutSekunder
    )

    LogWrapper(RapidApplication.create(env), meldingMediator).apply {
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
        Inntektsmeldinger(this, inntektsmeldingMediator)
        PersoninfoBeriker(this, personBerikerMediator, inntektsmeldingMediator)
        PersoninfoBerikerRetry(this, meldingMediator)
        Puls(this, inntektsmeldingMediator)
        AvbrutteSøknader(this, meldingMediator)
        AvbrutteArbeidsledigSøknader(this, meldingMediator)
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
