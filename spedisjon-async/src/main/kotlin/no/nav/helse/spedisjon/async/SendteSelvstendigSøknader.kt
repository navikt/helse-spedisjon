package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.JsonNode
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers.asLocalDateTime
import com.github.navikt.tbd_libs.rapids_and_rivers.isMissingOrNull
import com.github.navikt.tbd_libs.rapids_and_rivers.toUUID
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry

internal class SendteSelvstendigSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingMediator: MeldingMediator
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name")
                it.requireValue("status", "SENDT")
                it.requireValue("type", "SELVSTENDIGE_OG_FRILANSERE")
                it.requireAny("arbeidssituasjon", listOf("SELVSTENDIG_NARINGSDRIVENDE", "BARNEPASSER"))
                it.requireKey("sendtNav")
                it.require("fnr") { fnr ->
                    if (Integer.parseInt(fnr.asText().substring(0..1)) !in 30..31) throw IllegalStateException("Skal ikke behandle dette fnr enda.")
                }
                it.require("selvstendigNaringsdrivende.ventetid") { ventetid ->
                    if (ventetid.isMissingOrNull()) throw IllegalStateException("Mangler ventetid for selvstendig næringsdrivende")
                }
            }
            validate {
                it.requireKey("soknadsperioder")
                it.require("opprettet", JsonNode::asLocalDateTime)
                it.requireKey("id", "fom", "tom", "sykmeldingId")
                it.require("sendtNav", JsonNode::asLocalDateTime)
                it.interestedIn("utenlandskSykmelding", "sendTilGosys")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val detaljer = Meldingsdetaljer.sendtSøknadSelvstendig(packet)
        meldingMediator.leggInnMelding(detaljer).also { internId ->
            meldingMediator.onMelding(Melding.SendtSøknad(internId, packet["sykmeldingId"].asText().toUUID(), detaljer))
        }
    }

    override fun onError(problems: MessageProblems, context: MessageContext, metadata: MessageMetadata) {
        meldingMediator.onRiverError("kunne ikke gjenkjenne Sendt selvstendig søknad:\n$problems")
    }
}
