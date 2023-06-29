package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class AndreSøknaderRiver(
    rapidsConnection: RapidsConnection,
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate {
                it.rejectKey("@event_name", "inntektsmeldingId")
                it.rejectValue("type", "ARBEIDSTAKERE")
                it.requireKey("id", "fnr", "status")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        tjenestekall.info("Mottok søknad vi _ikke_ behandler med {}, {}, {} for {}:\n\t${packet.toJson().utenSpørsmål}",
            keyValue("søknadstype", packet["type"].asText()),
            keyValue("søknadsstatus", packet["Status"].asText()),
            keyValue("søknadId", packet["id"].asText()),
            keyValue("fødselsnummer", packet["fnr"].asText())
        )
    }

    private companion object {
        private val objectmapper = jacksonObjectMapper()
        private val tjenestekall = LoggerFactory.getLogger("tjenestekall")
        private val String.utenSpørsmål get() = (objectmapper.readTree(this) as ObjectNode).without<ObjectNode>("sporsmal")
    }
}
