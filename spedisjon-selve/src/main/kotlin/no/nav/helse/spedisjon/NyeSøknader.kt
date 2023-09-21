package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class NyeSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "NY")
                it.demandValue("type", "ARBEIDSTAKERE")
                it.requireKey("fnr", "arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fom", "tom")
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val nySøknadMelding = Melding.NySøknad(packet)
        meldingMediator.onMelding(nySøknadMelding, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Ny søknad:\n$problems")
    }
}
