package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

class PersoninfoBeriker(rapidsConnection: RapidsConnection): River.PacketListener {

    companion object {
        private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
    }
    internal var lestMelding = false

    init {
        River(rapidsConnection).apply {
            validate {
                it.demandKey("@løsning")
                it.demandAll("@behov", listOf("HentPersoninfoV3"))
                it.requireKey("spedisjonMeldingId", "HentPersoninfoV3.ident")
                it.requireKey("@løsning.HentPersoninfoV3.aktørId")
                it.require("@løsning.HentPersoninfoV3.fødselsdato") { JsonNode::asLocalDate }
             }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val aktørId = packet["@løsning.HentPersoninfoV3.aktørId"].asText()
        val fødselsdato = packet["@løsning.HentPersoninfoV3.fødselsdato"].asLocalDate()
        val ident = packet["HentPersoninfoV3.ident"].asText()
        val spedisjonMeldingId = packet["spedisjonMeldingId"].asText()
        tjenestekallLog.info("Mottok personinfoberikelse for aktørId=$aktørId med ident=$ident, fødselsdato=$fødselsdato og meldingId=$spedisjonMeldingId")
        lestMelding = true
    }
}
