package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

class FremtidigSøknaderRiver internal constructor(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "FREMTIDIG")
                it.requireKey("aktorId", "arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fom", "tom")
                it.interestedIn("fnr")
            }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Fremtidig søknad:\n$problems")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        meldingMediator.onPacket(packet, "aktorId", "fnr")
        meldingMediator.onMelding(Melding.NySøknad(packet), context)
    }

}
