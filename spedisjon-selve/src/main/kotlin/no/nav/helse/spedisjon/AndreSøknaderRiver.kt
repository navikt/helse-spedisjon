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
                if (System.getenv("NAIS_CLUSTER_NAME") == "dev-gcp") {
                    // vi støtter frilans og selvstendig i dev
                    it.rejectValue("type", "SELVSTENDIGE_OG_FRILANSERE")
                }
                it.requireKey("id", "fnr", "status")
                it.interestedIn("arbeidssituasjon", "arbeidsgiver.orgnummer")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
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
