package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.MissingNode
import no.nav.helse.rapids_rivers.*
import org.slf4j.LoggerFactory

internal class PersoninfoBeriker(
    rapidsConnection: RapidsConnection,
    private val personBerikerMediator: PersonBerikerMediator,
    private val inntektsmeldingMediator: InntektsmeldingMediator? = null
): River.PacketListener {

    companion object {
        private val tjenestekallLog = LoggerFactory.getLogger("tjenestekall")
    }
    init {
        River(rapidsConnection).apply {
            validate {
                it.demandKey("@løsning")
                it.demandAll("@behov", listOf("HentPersoninfoV3"))
                it.demandValue("@final", true)
                it.requireKey("spedisjonMeldingId", "HentPersoninfoV3.ident")
                it.requireKey("@løsning.HentPersoninfoV3.aktørId")
                it.require("@løsning.HentPersoninfoV3.fødselsdato") { JsonNode::asLocalDate }
                it.interestedIn("@løsning.HentPersoninfoV3.dødsdato") { JsonNode::asLocalDate }
                it.interestedIn("@løsning.HentPersoninfoV3.støttes", "@løsning.HentPersoninfoV3.historiskeFolkeregisteridenter")
             }
        }.register(this)
    }

    override fun onError(problems: MessageProblems, context: MessageContext) {
        tjenestekallLog.info("Forstod ikke personinfoberikelse: ${problems.toExtendedReport()}")
    }

    override fun onPacket(packet: JsonMessage, context: MessageContext) {
        val aktørId = packet["@løsning.HentPersoninfoV3.aktørId"].asText()
        val fødselsdato = packet["@løsning.HentPersoninfoV3.fødselsdato"].asLocalDate()
        val dødsdato = packet["@løsning.HentPersoninfoV3.dødsdato"].asOptionalLocalDate()
        val støttes = when(packet["@løsning.HentPersoninfoV3.støttes"]) {
            is MissingNode -> true
            else -> packet["@løsning.HentPersoninfoV3.støttes"].asBoolean()
        }
        val historiskeFolkeregisteridenter = packet["@løsning.HentPersoninfoV3.historiskeFolkeregisteridenter"].map(JsonNode::asText)
        val ident = packet["HentPersoninfoV3.ident"].asText()
        val spedisjonMeldingId = packet["spedisjonMeldingId"].asText()
        tjenestekallLog.info("Mottok personinfoberikelse for aktørId=$aktørId med ident=$ident, fødselsdato=$fødselsdato og spedisjonMeldingId=$spedisjonMeldingId\n${packet.toJson()}")
        val berikelse = Berikelse(fødselsdato, dødsdato, aktørId, støttes, historiskeFolkeregisteridenter, spedisjonMeldingId)
        personBerikerMediator.onPersoninfoBerikelse(spedisjonMeldingId, berikelse, context)

        val erDev = System.getenv()["NAIS_CLUSTER_NAME"] == "dev-gcp"
        if (erDev) inntektsmeldingMediator?.ekspeder(context) // ikke vent på puls
    }
}
