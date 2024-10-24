package no.nav.helse.opprydding

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory

internal class SlettPersonRiver(
    rapidsConnection: RapidsConnection,
    private val personRepository: PersonRepository
): River.PacketListener {

    private companion object {
        private val sikkerlogg = LoggerFactory.getLogger("tjenestekall")
    }

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandValue("@event_name", "slett_person")
                it.requireKey("@id", "fødselsnummer")
            }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val fødselsnummer = packet["fødselsnummer"].asText()
        sikkerlogg.info("Sletter dokumenter knyttet til person med fødselsnummer: $fødselsnummer")
        personRepository.slett(fødselsnummer)
        sikkerlogg.info("Dokumenter knyttet til person med fødselsnummer $fødselsnummer er slettet, sender kvittering")
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
