package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class AvbrutteArbeidsledigSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandAny("status", listOf("AVBRUTT", "UTGATT"))
                it.demandValue("type", "ARBEIDSLEDIG")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fnr", "fom", "tom")
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val avbruttSøknadMelding = Melding.AvbruttArbeidsledigSøknad(packet)
        meldingMediator.onMelding(avbruttSøknadMelding, context)
        sikkerlogg.info("Mottatt avbrutt arbeidsledig-søknad: $avbruttSøknadMelding")
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Avbrutt søknad:\n$problems")
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

}