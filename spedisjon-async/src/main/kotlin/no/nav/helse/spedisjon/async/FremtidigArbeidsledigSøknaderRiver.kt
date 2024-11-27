package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

class FremtidigArbeidsledigSøknaderRiver internal constructor(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    companion object {
        private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireValue("status", "FREMTIDIG")
                it.requireValue("type", "ARBEIDSLEDIG")
            }
            validate {
                it.requireKey("fnr", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fom", "tom")
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Fremtidig arbeidsledig søknad:\n$problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        tjenestekallLog.info("Behandler fremtidig arbeidsledig søknad: ${packet.toJson()}")

        // Innad i domenet vårt skiller vi ikke mellom fremtidige og nye søknader,
        // derfor gir det mening å maskere fremtidig søknad som ny, for å unngå to identiske håndteringer nedover i løpya
        packet["status"] = "NY"
        packet["fremtidig_søknad"] = true

        val detaljer = Meldingsdetaljer.nySøknadArbeidsledig(packet)
        val internId = meldingMediator.leggInnMelding(detaljer)
        meldingMediator.onMelding(Melding.NySøknad(internId, detaljer))
    }

}
