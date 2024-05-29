package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class Inntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val inntektsmeldingMediator: InntektsmeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandKey("inntektsmeldingId")
                it.demandValue("matcherSpleis", true)
                it.requireKey(
                    "inntektsmeldingId", "virksomhetsnummer",
                    "arbeidsgivertype", "beregnetInntekt",
                    "status", "arkivreferanse", "arbeidstakerFnr"
                )
                it.requireArray("arbeidsgiverperioder") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
                it.requireArray("ferieperioder") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
                it.requireArray("endringIRefusjoner") {
                    require("endringsdato", JsonNode::asLocalDate)
                }
                it.interestedIn("foersteFravaersdag", JsonNode::asLocalDate)
                it.interestedIn("refusjon.opphoersdato", JsonNode::asLocalDate)
                it.require("mottattDato", JsonNode::asLocalDateTime)
                it.interestedIn("refusjon.beloepPrMnd", "arbeidstakerAktorId", "arbeidsforholdId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val inntektsmelding = Melding.Inntektsmelding(packet)
        inntektsmeldingMediator.lagreInntektsmelding(inntektsmelding, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        inntektsmeldingMediator.onRiverError("kunne ikke gjenkjenne Inntektsmelding:\n$problems")
    }
}
