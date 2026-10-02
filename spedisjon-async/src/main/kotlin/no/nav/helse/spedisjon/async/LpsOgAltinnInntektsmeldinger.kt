package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import tools.jackson.databind.JsonNode

internal class LpsOgAltinnInntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition {
                    it.forbid("@event_name")
                    it.requireValue("format", "Inntektsmelding")
                }
                validate {
                    it.requireKey("inntektsmeldingId", "arkivreferanse", "arbeidstakerFnr", "virksomhetsnummer")
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
        val detaljer =
            Meldingsdetaljer(
                type = "inntektsmelding",
                fnr = packet["arbeidstakerFnr"].asString(),
                eksternDokumentId = packet["inntektsmeldingId"].asString().toUUID(),
                duplikatnøkkel = listOf(packet["arkivreferanse"].asString()),
                jsonBody = packet.toJson()
            )
        meldingMediator.leggInnMelding(detaljer).also { internId ->
            val inntektsmelding =
                Melding.Inntektsmelding(
                    internId = internId,
                    orgnummer = packet["virksomhetsnummer"].asString(),
                    arbeidsforholdId = packet["arbeidsforholdId"].takeIf(JsonNode::isString)?.asString(),
                    meldingsdetaljer = detaljer
                )
            meldingMediator.onMelding(inntektsmelding)
        }
    }

    override fun onError(
        problems: MessageProblems,
        context: MessageContext,
        metadata: MessageMetadata
    ) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne LPS/Altinn-Inntektsmelding:\n$problems")
    }
}
