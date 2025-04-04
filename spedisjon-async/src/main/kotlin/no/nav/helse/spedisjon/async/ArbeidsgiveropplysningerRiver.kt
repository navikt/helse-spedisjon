package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.*
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

sealed interface Arbeidsgiveropplysninger {
    val videresendingstype: String
    fun precondition(packet: JsonMessage)

    data object Forespurte: Arbeidsgiveropplysninger {
        override val videresendingstype = "arbeidsgiveropplysninger"

        override fun precondition(packet: JsonMessage) {
            packet.requireValue("forespurt", true)
            packet.requireValue("arsakTilInnsending", "Ny")
        }
    }

    data object Selvbestemte: Arbeidsgiveropplysninger {
        override val videresendingstype = "selvbestemte_arbeidsgiveropplysninger"

        override fun precondition(packet: JsonMessage) {
            packet.requireValue("forespurt", false)
        }
    }

    data object Korrigerte: Arbeidsgiveropplysninger {
        override val videresendingstype = "korrigerte_arbeidsgiveropplysninger"

        override fun precondition(packet: JsonMessage) {
            packet.requireValue("forespurt", true)
            packet.requireValue("arsakTilInnsending", "Endring")
        }
    }
}

internal class ArbeidsgiveropplysningerRiver(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator,
    private val arbeidsgiveropplysning: Arbeidsgiveropplysninger
) : River.PacketListener {
    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireValue("format", "Arbeidsgiveropplysninger")
                arbeidsgiveropplysning.precondition(it)
            }
            validate {
                it.requireKey("virksomhetsnummer", "vedtaksperiodeId", "arkivreferanse", "arbeidstakerFnr", "inntektsmeldingId")
                it.require("mottattDato", JsonNode::asLocalDateTime)
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
            type = arbeidsgiveropplysning.videresendingstype,
            fnr = packet["arbeidstakerFnr"].asText(),
            eksternDokumentId = packet["inntektsmeldingId"].asText().toUUID(),
            rapportertDato = packet["mottattDato"].asLocalDateTime(),
            duplikatnøkkel = listOf(packet["arkivreferanse"].asText()),
            jsonBody = packet.toJson()
        )
        sikkerlogg.info("håndterer ${arbeidsgiveropplysning::class.simpleName} arbeidsgiveropplysninger\n\t$detaljer")

        meldingMediator.leggInnMelding(detaljer).also { internId ->
            val inntektsmelding = Melding.Arbeidsgiveropplysninger(
                internId = internId,
                meldingsdetaljer = detaljer
            )
            meldingMediator.onMelding(inntektsmelding)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne ${arbeidsgiveropplysning::class.simpleName} arbeidsgiveropplysninger:\n\t$problems")
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }
}
