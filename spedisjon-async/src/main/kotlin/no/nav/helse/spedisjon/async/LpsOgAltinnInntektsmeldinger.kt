package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.*
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class LpsOgAltinnInntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
    private val inntektsmeldingMediator: InntektsmeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireKey("inntektsmeldingId")
                it.forbidValues("avsenderSystem.navn", listOf("NAV_NO", "NAV_NO_SELVBESTEMT"))
            }
            validate {
                it.requireKey(
                    "inntektsmeldingId", "virksomhetsnummer",
                    "arbeidsgivertype", "beregnetInntekt",
                    "status", "arkivreferanse", "arbeidstakerFnr",
                    "matcherSpleis"
                )
                it.requireArray("arbeidsgiverperioder") {
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

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry
    ) {
        if (!packet["matcherSpleis"].asBoolean()) return sikkerlogg.info("Ignorerer LPS/Altinn-inntektsmelding som ikke matcher spleis:\n\t ${packet.toJson()}")

        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = packet["arbeidstakerFnr"].asText(),
            eksternDokumentId = packet["inntektsmeldingId"].asText().toUUID(),
            rapportertDato = packet["mottattDato"].asLocalDateTime(),
            duplikatnøkkel = listOf(packet["arkivreferanse"].asText()),
            jsonBody = packet.toJson()
        )
        meldingMediator.leggInnMelding(detaljer).also { internId ->
            val inntektsmelding = Melding.Inntektsmelding(
                internId = internId,
                orgnummer = packet["virksomhetsnummer"].asText(),
                arbeidsforholdId = packet["arbeidsforholdId"].takeIf(JsonNode::isTextual)?.asText(),
                meldingsdetaljer = detaljer
            )
            meldingMediator.onMelding(inntektsmelding)
            inntektsmeldingMediator.lagreInntektsmelding(inntektsmelding)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne LPS/Altinn-Inntektsmelding:\n$problems")
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
}
