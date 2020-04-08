package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class NyeSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("aktorId", "arbeidsgiver.orgnummer", "soknadsperioder") }
            validate { it.require("opprettet", JsonNode::asLocalDateTime) }
            validate { it.requireValue("status", "NY") }
            validate { it.requireKey("id", "sykmeldingId", "fom", "tom") }
            validate { it.interestedIn("fnr") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        meldingMediator.onPacket(packet, "aktorId", "fnr")
        meldingMediator.onMelding(Melding.NySøknad(packet), context)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        meldingMediator.onRiverError("Ny søknad", problems)
    }
}
