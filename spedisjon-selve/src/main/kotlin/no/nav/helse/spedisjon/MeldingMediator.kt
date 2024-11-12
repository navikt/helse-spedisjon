package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.withMDC
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import no.nav.helse.spedisjon.Personinformasjon.Companion.berikMeldingOgBehandleDen
import org.slf4j.LoggerFactory
import java.util.UUID

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val speedClient: SpeedClient
) {
    internal companion object {
        private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        private val logg = LoggerFactory.getLogger(MeldingMediator::class.java)
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
    }

    private var messageRecognized = false
    private val riverErrors = mutableListOf<String>()

    fun beforeMessage() {
        messageRecognized = false
        riverErrors.clear()
    }

    fun onRiverError(error: String) {
        riverErrors.add(error)
    }

    fun onMelding(melding: Melding, context: MessageContext) {
        messageRecognized = true

        Counter.builder("melding_totals")
            .description("Antall meldinger mottatt")
            .tag("type", melding.type)
            .register(registry)
            .increment()

        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen

        berikOgSendVidere(melding, context)

        Counter.builder("melding_unik_totals")
            .description("Antall unike meldinger mottatt")
            .tag("type", melding.type)
            .register(registry)
            .increment()

        Counter.builder("melding_sendt_totals")
            .description("Antall meldinger sendt")
            .tag("type", melding.type)
            .register(registry)
            .increment()
    }

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun berikOgSendVidere(melding: Melding, context: MessageContext) {
        // vi sender ikke inntektsmelding  videre. her er vi avhengig av puls!
        if (melding is Melding.Inntektsmelding) return

        berikMeldingOgBehandleDen(speedClient, melding) { berikelse ->
            val beriketMelding = berikelse.berik(melding)
            context.publish(melding.fødselsnummer(), beriketMelding.toJson())
        }
    }
}
