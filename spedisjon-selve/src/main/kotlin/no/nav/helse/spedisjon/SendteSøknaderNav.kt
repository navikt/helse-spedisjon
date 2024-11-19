package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry

internal class SendteSøknaderNav(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireValue("status", "SENDT")
                it.requireValue("type", "ARBEIDSTAKERE")
                it.requireKey("sendtNav")
            }
            validate {
                it.requireKey("arbeidsgiver.orgnummer", "soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "sykmeldingId", "fnr", "fom", "tom", "fravar")
                it.require("sendtNav", JsonNode::asLocalDateTime)
                it.interestedIn("utenlandskSykmelding", "sendTilGosys")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val detaljer = Meldingsdetaljer.sendtSøknadNav(packet)
        meldingMediator.leggInnMelding(detaljer)?.also { internId ->
            meldingMediator.onMelding(Melding.SendtSøknad(internId, packet["sykmeldingId"].asText().toUUID(), detaljer), context)
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt søknad Nav:\n$problems")
    }
}
