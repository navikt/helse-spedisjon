package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory

/**
 * En avbrutt søknad er en søknad bruker velger aktivt å ikke bruke.
 *
 * Til sammenlikning er en utgått søknad en søknad bruker ikke har brukt innen fristen (som er på 10mnd?)
 */
internal class AvbrutteSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) :
    River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name")
                it.demandAny("status", listOf("AVBRUTT", "UTGATT"))
                it.demandValue("type", "ARBEIDSTAKERE")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fnr", "fom", "tom", "arbeidsgiver.orgnummer")
                it.interestedIn("aktorId")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val avbruttSøknadMelding = Melding.AvbruttSøknad(packet)
        meldingMediator.onMelding(avbruttSøknadMelding, context)
        sikkerlogg.info("Mottatt avbrutt søknad: $avbruttSøknadMelding")
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Avbrutt søknad:\n$problems")
    }

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

}