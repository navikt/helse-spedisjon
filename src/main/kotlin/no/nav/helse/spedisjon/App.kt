package no.nav.helse.spedisjon

import io.ktor.util.KtorExperimentalAPI
import no.nav.helse.rapids_rivers.RapidApplication

@KtorExperimentalAPI
fun main() {
    val env = System.getenv()
    val dataSourceBuilder = DataSourceBuilder(env)
    dataSourceBuilder.migrate()
    val meldingDao = MeldingDao(dataSourceBuilder.getDataSource())

    RapidApplication.create(env) { _, rapid -> rapid.seekToBeginning() }.apply {
        NyeSøknader(this, meldingDao)
        SendteSøknader(this, meldingDao)
        Inntektsmeldinger(this, meldingDao)
    }.start()
}
