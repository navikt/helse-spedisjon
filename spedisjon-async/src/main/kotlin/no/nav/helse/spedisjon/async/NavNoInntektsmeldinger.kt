package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.*
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

internal class NavNoInntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireKey("inntektsmeldingId")
                it.requireAny("avsenderSystem.navn", listOf("NAV_NO", "NAV_NO_SELVBESTEMT"))
            }
            validate {
                it.requireKey("inntektsmeldingId", "arkivreferanse", "arbeidstakerFnr", "virksomhetsnummer")
                it.require("mottattDato", JsonNode::asLocalDateTime)
                it.interestedIn("arbeidsforholdId")
            }
        }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry
    ) {
        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = packet["arbeidstakerFnr"].asText(),
            eksternDokumentId = packet["inntektsmeldingId"].asText().toUUID(),
            rapportertDato = packet["mottattDato"].asLocalDateTime(),
            duplikatnøkkel = listOf(packet["arkivreferanse"].asText()),
            jsonBody = packet.toJson()
        )
        sikkerlogg.info("håndterer Nav.no-inntektsmelding\n$detaljer")
        meldingMediator.leggInnMelding(detaljer).also { internId ->
            val inntektsmelding = Melding.NavNoInntektsmelding(
                internId = internId,
                orgnummer = packet["virksomhetsnummer"].asText(),
                arbeidsforholdId = packet["arbeidsforholdId"].takeIf(JsonNode::isTextual)?.asText(),
                meldingsdetaljer = detaljer
            )
            meldingMediator.onMelding(inntektsmelding)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Nav.no-Inntektsmelding:\n$problems")
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
}
