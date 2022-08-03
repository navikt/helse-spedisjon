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
                it.demandValue("type", "ARBEIDSTAKERE")
                it.requireKey("fnr", "arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fom", "tom", "egenmeldinger", "fravar")
                it.require("sendtArbeidsgiver", JsonNode::asLocalDateTime)
                it.interestedIn("aktorId")
                it.rejectValue("sendTilGosys", true)
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val søknad = Melding.SendtSøknadArbeidsgiver(packet)
        meldingMediator.onMeldingAsync(søknad, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt søknad arbeidsgiver:\n$problems")
    }
}
