package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class Inntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate {
                it.requireKey(
                    "inntektsmeldingId",
                    "arbeidstakerAktorId", "virksomhetsnummer",
                    "arbeidsgivertype", "beregnetInntekt",
                    "endringIRefusjoner", "arbeidsgiverperioder",
                    "status", "arkivreferanse", "ferieperioder"
                )
            }
            validate { it.require("foersteFravaersdag", JsonNode::asLocalDate) }
            validate { it.require("mottattDato", JsonNode::asLocalDateTime) }
            validate { it.interestedIn("arbeidstakerFnr") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        meldingMediator.onPacket(packet, "arbeidstakerAktorId", "arbeidstakerFnr")
        meldingMediator.onMelding(Melding.Inntektsmelding(packet), context)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        meldingMediator.onRiverError("Inntektsmelding", problems)
    }
}
