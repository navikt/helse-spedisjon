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
                    "inntektsmeldingId", "arbeidstakerAktorId", "virksomhetsnummer",
                    "arbeidsgivertype", "beregnetInntekt",
                    "status", "arkivreferanse"
                )
            }
            validate {
                it.requireArray("arbeidsgiverperioder") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
            }
            validate {
                it.requireArray("ferieperioder") {
                    require("fom", JsonNode::asLocalDate)
                    require("tom", JsonNode::asLocalDate)
                }
            }
            validate {
                it.requireArray("endringIRefusjoner") {
                    require("endringsdato", JsonNode::asLocalDate)
                }
            }
            validate { it.interestedIn("foersteFravaersdag", JsonNode::asLocalDate) }
            validate { it.interestedIn("refusjon.opphoersdato", JsonNode::asLocalDate) }
            validate { it.require("mottattDato", JsonNode::asLocalDateTime) }
            validate { it.interestedIn("arbeidstakerFnr", "refusjon.beloepPrMnd") }
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
