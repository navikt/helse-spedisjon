package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*

internal class NyeFrilansSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandValue("status", "NY")
                it.demandValue("type", "SELVSTENDIGE_OG_FRILANSERE")
                it.demandValue("arbeidssituasjon", "FRILANSER")
                it.requireKey("fnr", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fom", "tom")
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val nySøknadMelding = Melding.NyFrilansSøknad(packet)
        meldingMediator.onMelding(nySøknadMelding, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Ny frilans søknad:\n$problems")
    }
}
