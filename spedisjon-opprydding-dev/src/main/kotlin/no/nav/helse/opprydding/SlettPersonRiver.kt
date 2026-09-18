package no.nav.helse.opprydding

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageContext
import com.github.navikt.tbd_libs.rapids_and_rivers_api.RapidsConnection
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageMetadata
import com.github.navikt.tbd_libs.rapids_and_rivers.River
import io.micrometer.core.instrument.MeterRegistry
import org.intellij.lang.annotations.Language
import no.nav.sykepenger.libs.logging.loggInfo

internal class SlettPersonRiver(
    rapidsConnection: RapidsConnection,
    private val personRepository: PersonRepository
): River.PacketListener {
    init {
        River(rapidsConnection).apply {
            precondition { it.requireValue("@event_name", "slett_person") }
            validate {
                it.requireKey("@id", "fødselsnummer")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext, metadata: MessageMetadata, meterRegistry: MeterRegistry) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        loggInfo("Sletter dokumenter knyttet til person", "fødselsnummer" to fødselsnummer)
        personRepository.slett(fødselsnummer)
        loggInfo("Dokumenter knyttet til person er slettet, sender kvittering", "fødselsnummer" to fødselsnummer)
        context.publish(fødselsnummer, lagPersonSlettet(fødselsnummer))
    }

    @Language("JSON")
    private fun lagPersonSlettet(fødselsnummer: String) = """
        {
            "@event_name": "person_slettet",
            "fødselsnummer": "$fødselsnummer"
        }  
    """.trimIndent()
}
