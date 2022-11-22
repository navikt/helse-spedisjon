package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import javax.sql.DataSource

internal class InntektsmeldingDao(dataSource: DataSource): AbstractDao(dataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()
    }

    fun leggInn(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime): Boolean {
        log.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding, ønsketPublisert).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    fun hentUsendteMeldinger(): List<Triple<String, String, JsonNode>> {
        return """SELECT i.fnr, i.orgnummer, m.data FROM inntektsmelding i JOIN melding m ON i.duplikatkontroll = m.duplikatkontroll WHERE i.republisert IS NULL AND i.timeout < :timeout"""
            .listQuery<Triple<String, String, JsonNode>>(mapOf("timeout" to LocalDateTime.now()))
            { row -> Triple(row.string("fnr"), row.string("orgnummer"), objectMapper.readTree(row.string("data"))) }
    }

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "mottatt" to LocalDateTime.now(),
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1

}
