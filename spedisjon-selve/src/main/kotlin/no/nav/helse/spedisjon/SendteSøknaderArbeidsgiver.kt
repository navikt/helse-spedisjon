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
                it.requireKey("id", "sykmeldingId", "fom", "tom", "fravar")
                it.require("sendtArbeidsgiver", JsonNode::asLocalDateTime)
                it.interestedIn("aktorId", "utenlandskSykmelding", "sendTilGosys")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val søknad = Melding.SendtSøknadArbeidsgiver(packet)
        meldingMediator.onMelding(søknad, context)
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt søknad arbeidsgiver:\n$problems")
    }
}
