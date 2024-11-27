package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import java.util.*

internal class MeldingMediator(
    private val meldingtjeneste: Meldingtjeneste,
    private val speedClient: SpeedClient,
    private val ekspederingMediator: EkspederingMediator
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

    fun leggInnMelding(meldingsdetaljer: Meldingsdetaljer): UUID? {
        Counter.builder("melding_totals")
            .description("Antall meldinger mottatt")
            .tag("type", meldingsdetaljer.type)
            .register(registry)
            .increment()
        val request = NyMeldingRequest(
            type = meldingsdetaljer.type,
            fnr = meldingsdetaljer.fnr,
            eksternDokumentId = meldingsdetaljer.eksternDokumentId,
            rapportertDato = meldingsdetaljer.rapportertDato,
            duplikatkontroll = meldingsdetaljer.duplikatkontroll,
            jsonBody = meldingsdetaljer.jsonBody
        )
        return when (val response = meldingtjeneste.nyMelding(request)) {
            NyMeldingResponse.Duplikatkontroll -> null
            is NyMeldingResponse.OK -> response.internDokumentId
        }
    }

    fun onMelding(melding: Melding) {
        messageRecognized = true
        berikOgSendVidere(melding)

        Counter.builder("melding_unik_totals")
            .description("Antall unike meldinger mottatt")
            .tag("type", melding.meldingsdetaljer.type)
            .register(registry)
            .increment()

        Counter.builder("melding_sendt_totals")
            .description("Antall meldinger sendt")
            .tag("type", melding.meldingsdetaljer.type)
            .register(registry)
            .increment()
    }

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun berikOgSendVidere(melding: Melding) {
        // vi sender ikke inntektsmelding  videre. her er vi avhengig av puls!
        if (melding is Melding.Inntektsmelding) return

        Personinformasjon.Companion.berikMeldingOgBehandleDen(speedClient, melding) { berikelse ->
            val beriketMelding = berikelse.berik(melding)
            ekspederingMediator.videresendMelding(melding.meldingsdetaljer.fnr, melding.internId, beriketMelding)
        }
    }
}
