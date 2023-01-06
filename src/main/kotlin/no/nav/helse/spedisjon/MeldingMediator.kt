package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import io.prometheus.client.Counter
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val berikelseDao: BerikelseDao,
) {
    internal companion object {
        private val logg = LoggerFactory.getLogger(MeldingMediator::class.java)
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
        private val meldingsteller = Counter.build("melding_totals", "Antall meldinger mottatt")
            .labelNames("type")
            .register()
        private val unikteller = Counter.build("melding_unik_totals", "Antall unike meldinger mottatt")
            .labelNames("type")
            .register()
        private val sendtteller = Counter.build("melding_sendt_totals", "Antall meldinger sendt")
            .labelNames("type")
            .register()

    }

    private var messageRecognized = false
    private val riverErrors = mutableListOf<String>()

    fun beforeMessage() {
        messageRecognized = false
        riverErrors.clear()
    }

    fun onRiverError(error: String) {
        riverErrors.add(error)
    }

    fun onMelding(melding: Melding, context: MessageContext) {
        messageRecognized = true
        meldingsteller.labels(melding.type).inc()
        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        sendBehovÈnGang(melding.fødselsnummer(), listOf("aktørId", "fødselsdato", "støttes", "historiskeFolkeregisteridenter"), melding.duplikatkontroll(), context)
        unikteller.labels(melding.type).inc()
        sendtteller.labels(melding.type).inc()
    }

    fun afterMessage(message: String) {
        if (messageRecognized || riverErrors.isEmpty()) return
        sikkerLogg.warn("kunne ikke gjenkjenne melding:\n\t$message\n\nProblemer:\n${riverErrors.joinToString(separator = "\n")}")
    }

    private fun sendBehovÈnGang(fødselsnummer: String, behov: List<String>, duplikatkontroll: String, context: MessageContext) {
        if (berikelseDao.behovErEtterspurt(duplikatkontroll)) return // Om om vi allerede har etterspurt behov gjør vi det ikke på ny
        sendBehov(fødselsnummer, behov, duplikatkontroll, context)
    }

    private fun sendBehov(fødselsnummer: String, behov: List<String>, duplikatkontroll: String, context: MessageContext) {
        context.publish(fødselsnummer,
            JsonMessage.newNeed(behov = listOf("HentPersoninfoV3"),
                map = mapOf("HentPersoninfoV3" to mapOf(
                    "ident" to fødselsnummer,
                    "attributter" to behov,
                ), "spedisjonMeldingId" to duplikatkontroll)).toJson())
        berikelseDao.behovEtterspurt(fødselsnummer, duplikatkontroll, behov, LocalDateTime.now())
    }

    fun onPersoninfoBerikelse(
        context: MessageContext,
        fødselsnummer: String,
        beriketMelding: JsonNode
    ) {
        context.publish(fødselsnummer, beriketMelding.toString())
    }

    fun retryBehov(opprettetFør: LocalDateTime, context:MessageContext) {
        val ubesvarteBehov = berikelseDao.ubesvarteBehov(opprettetFør)
        logg.info("Er ${ubesvarteBehov.size} ubesvarte behov som er opprettet før $opprettetFør")
        ubesvarteBehov.forEach { ubesvartBehov ->
            ubesvartBehov.logg(logg)
            sendBehov(ubesvartBehov.fnr, ubesvartBehov.behov, ubesvartBehov.duplikatkontroll, context)
        }
    }
}
