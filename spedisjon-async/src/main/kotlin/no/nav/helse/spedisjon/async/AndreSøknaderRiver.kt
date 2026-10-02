package no.nav.helse.spedisjon.async

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import no.nav.sykepenger.libs.logging.loggInfo
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.kotlin.jacksonObjectMapper

internal class AndreSøknaderRiver(
    rapidsConnection: RapidsConnection
) : River.PacketListener {
    init {
        River(rapidsConnection)
            .apply {
                precondition {
                    it.forbid("@event_name", "inntektsmeldingId")
                    it.forbidValue("type", "ARBEIDSTAKERE")
                    it.forbidValue("type", "ARBEIDSLEDIG")
                    it.forbidValue("type", "SELVSTENDIGE_OG_FRILANSERE")
                }
                validate {
                    it.requireKey("id", "fnr", "status")
                    it.interestedIn("arbeidssituasjon", "arbeidsgiver.orgnummer")
                }
            }.register(this)
    }

    override fun onPacket(
        packet: JsonMessage,
        context: MessageContext,
        metadata: MessageMetadata,
        meterRegistry: MeterRegistry
    ) {
        try {
            loggInfo(
                "Mottok søknad vi _ikke_ behandler",
                "søknadstype" to packet["type"].asString(),
                "søknadsstatus" to packet["status"].asString(),
                "arbeidssituasjon" to packet["arbeidssituasjon"].asString("IKKE_SATT"),
                "søknadId" to packet["id"].asString(),
                "fødselsnummer" to packet["fnr"].asString(),
                "orgnummer" to packet["arbeidsgiver.orgnummer"].asString("IKKE_SATT"),
                "søknad" to packet.toJson().utenStøy.toString()
            )
        } catch (ex: Exception) {
            loggInfo("Feil ved logging av søknad vi ikke behandler", ex)
        }
    }

    private companion object {
        private val objectmapper = jacksonObjectMapper()
        private val støy = setOf("sporsmal", "system_participating_services", "system_read_count", "@opprettet", "@id")
        private val String.utenStøy get() = (objectmapper.readTree(this) as ObjectNode).without(støy)
    }
}
