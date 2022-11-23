package no.nav.helse.spedisjon

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
    // formål: hente ut de meldingene vi mener vi skal sende videre - de må være beriket
    fun hentUsendteMeldinger(): List<SendeklarInntektsmelding> {
        return """SELECT i.fnr, i.orgnummer, m.data, b.løsning 
            FROM inntektsmelding i 
            JOIN melding m ON i.duplikatkontroll = m.duplikatkontroll 
            JOIN berikelse b ON i.duplikatkontroll = b.duplikatkontroll
            WHERE i.republisert IS NULL AND i.timeout < :timeout AND b.løsning IS NOT NULL""".trimMargin()
            .listQuery(mapOf("timeout" to LocalDateTime.now()))
            { row -> SendeklarInntektsmelding(row.string("fnr"), row.string("orgnummer"), Melding.les("inntektsmelding", row.string("data")) as Melding.Inntektsmelding, objectMapper.readTree(row.string("løsning"))) }
    }

    // ny funksjon som teller usendte meldinger basert på fødselsnummer og orgnummer slik at vi kan se om vi har flere enn en.
    // telle alle de vi har mottatt i løpet av perioden fra den meldingen vi ønsker å sende ut til nå
    // hendt usendt emeldinger gir oss en liste, og for hver melding i den lista, må vi kalle denne nye funksjonen

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "mottatt" to LocalDateTime.now(),
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1

}
