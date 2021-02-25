package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class SendteSøknaderArbeidsgiver(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.rejectKey("sendtNav")
                it.demandKey("sendtArbeidsgiver")
                it.demandValue("status", "SENDT")
                it.requireKey("aktorId", "arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fom", "tom", "egenmeldinger", "fravar")
                it.require("sendtArbeidsgiver", JsonNode::asLocalDateTime)
                it.interestedIn("fnr")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        meldingMediator.onPacket(packet, "aktorId", "fnr")
        meldingMediator.onMelding(Melding.SendtSøknadArbeidsgiver(packet), context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt søknad arbeidsgiver:\n$problems")
    }
}
