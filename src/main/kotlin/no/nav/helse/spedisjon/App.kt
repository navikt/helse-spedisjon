package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.helse.rapids_rivers.RapidsConnection

fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)
    val meldingDao = MeldingDao(dataSourceBuilder.getDataSource())

    RapidApplication.create(env).apply {
        NyeSøknader(this, meldingDao)
        SendteSøknaderArbeidsgiver(this, meldingDao)
        SendteSøknaderNav(this, meldingDao)
        Inntektsmeldinger(this, meldingDao)
    }.apply {
        register(object : RapidsConnection.StatusListener {
            override fun onStartup(rapidsConnection: RapidsConnection) {
                dataSourceBuilder.migrate()
            }
        })
    }.start()
}
