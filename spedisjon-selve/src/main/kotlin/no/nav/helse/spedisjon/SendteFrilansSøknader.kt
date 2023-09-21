package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class SendteFrilansSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "SENDT")
                it.demandValue("type", "SELVSTENDIGE_OG_FRILANSERE")
                it.demandValue("arbeidssituasjon", "FRILANSER")
                it.demandKey("sendtNav")
                it.requireKey("soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fnr", "fom", "tom")
                it.require("sendtNav", JsonNode::asLocalDateTime)
                it.interestedIn("aktorId", "utenlandskSykmelding", "sendTilGosys")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val søknad = Melding.SendtFrilansSøknad(packet)
        meldingMediator.onMelding(søknad, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt frilans søknad:\n$problems")
    }
}
