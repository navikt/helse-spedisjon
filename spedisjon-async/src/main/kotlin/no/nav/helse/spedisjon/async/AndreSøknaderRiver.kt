package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import io.micrometer.core.instrument.MeterRegistry
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory

internal class AndreSøknaderRiver(
    rapidsConnection: RapidsConnection,
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            precondition {
                it.forbid("@event_name", "inntektsmeldingId")
                it.forbidValue("type", "ARBEIDSTAKERE")
                it.forbidValue("type", "ARBEIDSLEDIG")
                if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
                    // vi støtter selvstendig og arbeidsledig i dev, men bare for ordinære Selvstendig næringsdrivende (ikke Jordbruker etc)
                    it.forbidValue("arbeidssituasjon", "SELVSTENDIG_NARINGSDRIVENDE")
                }
            }
            validate {
                it.requireKey("id", "fnr", "status")
                it.interestedIn("arbeidssituasjon", "arbeidsgiver.orgnummer")
            }
        }.register(this)
}

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        try {
            tjenestekall.info("Mottok søknad vi _ikke_ behandler med {}, {}, {}, {} for {} {}:\n\n\t${packet.toJson().utenStøy}",
                keyValue("søknadstype", packet["type"].asText()),
                keyValue("søknadsstatus", packet["status"].asText()),
                keyValue("arbeidssituasjon", packet["arbeidssituasjon"].asText("IKKE_SATT")),
                keyValue("søknadId", packet["id"].asText()),
                keyValue("fødselsnummer", packet["fnr"].asText()),
                keyValue("orgnummer", packet["arbeidsgiver.orgnummer"].asText("IKKE_SATT")),
            )
        } catch (ex: Exception) {
            tjenestekall.info("Feil ved logging av søknad vi ikke behandler", ex)
        }
    }

    private companion object {
        private val objectmapper = jacksonObjectMapper()
        private val tjenestekall = LoggerFactory.getLogger("tjenestekall")
        private val støy = setOf("sporsmal", "system_participating_services", "system_read_count", "@opprettet", "@id")
        private val String.utenStøy get() = (objectmapper.readTree(this) as ObjectNode).without<ObjectNode>(støy)
    }
}
