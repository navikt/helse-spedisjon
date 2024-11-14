package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDate
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.helse.rapids_rivers.*

internal class Inntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val inntektsmeldingMediator: InntektsmeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireKey("inntektsmeldingId")
                it.requireValue("matcherSpleis", true)
            }
            validate {
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
                it.interestedIn("refusjon.beloepPrMnd", "arbeidsforholdId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val inntektsmelding = Melding.Inntektsmelding(packet)
        inntektsmeldingMediator.lagreInntektsmelding(inntektsmelding, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        inntektsmeldingMediator.onRiverError("kunne ikke gjenkjenne Inntektsmelding:\n$problems")
    }
}
