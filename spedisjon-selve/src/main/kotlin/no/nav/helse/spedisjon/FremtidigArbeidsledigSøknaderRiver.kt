package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
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
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "FREMTIDIG")
                it.demandValue("type", "ARBEIDSLEDIG")
                it.requireKey("fnr", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fom", "tom")
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Fremtidig arbeidsledig søknad:\n$problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        tjenestekallLog.info("Behandler fremtidig arbeidsledig søknad: ${packet.toJson()}")

        // Innad i domenet vårt skiller vi ikke mellom fremtidige og nye søknader,
        // derfor gir det mening å maskere fremtidig søknad som ny, for å unngå to identiske håndteringer nedover i løpya
        packet["status"] = "NY"
        packet["fremtidig_søknad"] = true

        val nySøknadMelding = Melding.NyArbeidsledigSøknad(packet)
        meldingMediator.onMelding(nySøknadMelding, context)
    }

}
