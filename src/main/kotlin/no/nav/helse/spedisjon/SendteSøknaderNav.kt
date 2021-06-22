package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class SendteSøknaderNav(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "SENDT")
                it.demandValue("type", "ARBEIDSTAKERE")
                it.demandKey("sendtNav")
                it.requireKey("arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fnr", "fom", "tom", "egenmeldinger", "fravar")
                it.require("sendtNav", JsonNode::asLocalDateTime)
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        meldingMediator.onPacket(packet, "aktorId", "fnr")
        meldingMediator.onMelding(Melding.SendtSøknadNav(packet), context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt søknad Nav:\n$problems")
    }
}
