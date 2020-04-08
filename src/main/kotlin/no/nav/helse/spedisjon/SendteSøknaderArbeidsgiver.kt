package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class SendteSøknaderArbeidsgiver(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("aktorId", "arbeidsgiver.orgnummer", "soknadsperioder") }
            validate { it.require("opprettet", JsonNode::asLocalDateTime) }
            validate { it.requireValue("status", "SENDT") }
            validate { it.requireKey("id", "fom", "tom", "egenmeldinger", "fravar") }
            validate { it.require("sendtArbeidsgiver", JsonNode::asLocalDateTime) }
            validate { it.forbid("sendtNav") }
            validate { it.interestedIn("fnr") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        meldingMediator.onPacket(packet, "aktorId", "fnr")
        meldingMediator.onMelding(Melding.SendtSøknadArbeidsgiver(packet), context)
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        meldingMediator.onRiverError("Sendt søknad arbeidsgiver", problems)
    }
}
