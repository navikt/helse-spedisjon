package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.withMDC
import com.github.navikt.tbd_libs.speed.SpeedClient
import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import java.util.*
import org.slf4j.LoggerFactory

internal class MeldingMediator(
    private val meldingtjeneste: Meldingtjeneste,
    private val speedClient: SpeedClient,
    private val ekspederingMediator: EkspederingMediator
) {
    internal companion object {
        private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
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

    fun leggInnMelding(meldingsdetaljer: Meldingsdetaljer): UUID {
        Counter.builder("melding_totals")
            .description("Antall meldinger mottatt")
            .tag("type", meldingsdetaljer.type)
            .register(registry)
            .increment()

        messageRecognized = true

        return withMDC("ekstern_dokument_id" to "${meldingsdetaljer.eksternDokumentId}") {
            val request = NyMeldingRequest(
                type = meldingsdetaljer.type,
                fnr = meldingsdetaljer.fnr,
                eksternDokumentId = meldingsdetaljer.eksternDokumentId,
                rapportertDato = meldingsdetaljer.rapportertDato,
                duplikatkontroll = meldingsdetaljer.duplikatkontroll,
                jsonBody = meldingsdetaljer.jsonBody
            )
            meldingtjeneste.nyMelding(request).internDokumentId
        }
    }

    fun onMelding(melding: Melding) {
        when (melding) {
            is Melding.AvbruttSøknad,
            is Melding.NySøknad,
            is Melding.SendtSøknad -> {
                Personinformasjon.berikMeldingOgBehandleDen(speedClient, melding) { berikelse ->
                    if (melding.meldingsdetaljer.type == "sendt_søknad_selvstendig" && berikelse.fødselsdato.dayOfMonth in setOf(1, 31)) {
                        // unngå å sende søknader for de som er født mellom 1. og 31. i måneden lmao
                        return@berikMeldingOgBehandleDen
                    }
                    val beriketMelding = berikelse.berik(melding)
                    ekspederingMediator.videresendMelding(melding.meldingsdetaljer.fnr, melding.internId, beriketMelding)
                }
            }
            is Melding.Arbeidsgiveropplysninger -> {
                ekspederingMediator.videresendMelding(melding.meldingsdetaljer.fnr, melding.internId, BeriketMelding(melding.rapidhendelse))
            }
            is Melding.Inntektsmelding -> error("lps-inntektsmeldinger skal ikke sendes her")
        }

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
}
