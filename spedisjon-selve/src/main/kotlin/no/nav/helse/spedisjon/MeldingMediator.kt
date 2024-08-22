package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

internal class MeldingMediator(
    private val meldingDao: MeldingDao,
    private val berikelseDao: BerikelseDao,
) {
    internal companion object {
        private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        private val logg = LoggerFactory.getLogger(MeldingMediator::class.java)
        private val sikkerLogg = LoggerFactory.getLogger("tjenestekall")
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

        Counter.builder("melding_totals")
            .description("Antall meldinger mottatt")
            .tag("type", melding.type)
            .register(registry)
            .increment()

        if (!meldingDao.leggInn(melding)) return // Melding ignoreres om det er duplikat av noe vi allerede har i basen
        sendBehovÈnGang(melding.fødselsnummer(), listOf("aktørId", "fødselsdato", "støttes", "dødsdato", "historiskeFolkeregisteridenter"), melding.duplikatkontroll(), context)

        Counter.builder("melding_unik_totals")
            .description("Antall unike meldinger mottatt")
            .tag("type", melding.type)
            .register(registry)
            .increment()

        Counter.builder("melding_sendt_totals")
            .description("Antall meldinger sendt")
            .tag("type", melding.type)
            .register(registry)
            .increment()
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
        berikelseDao.behovEtterspurt(fødselsnummer, duplikatkontroll, behov, LocalDateTime.now())
        context.publish(fødselsnummer,
            JsonMessage.newNeed(behov = listOf("HentPersoninfoV3"),
                map = mapOf("HentPersoninfoV3" to mapOf(
                    "ident" to fødselsnummer,
                    "attributter" to behov,
                ), "spedisjonMeldingId" to duplikatkontroll)).toJson())
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
            ubesvartBehov.logg(sikkerLogg)
            sendBehov(ubesvartBehov.fnr, ubesvartBehov.behov, ubesvartBehov.duplikatkontroll, context)
        }
    }
}
