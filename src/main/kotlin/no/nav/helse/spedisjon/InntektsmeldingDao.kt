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

    fun leggInn(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime = LocalDateTime.now()): Boolean {
        log.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding, ønsketPublisert, mottatt).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    fun hentSendeklareMeldinger(): List<SendeklarInntektsmelding> {
        return """SELECT i.fnr, i.orgnummer, i.mottatt, m.data, b.løsning 
            FROM inntektsmelding i 
            JOIN melding m ON i.duplikatkontroll = m.duplikatkontroll 
            JOIN berikelse b ON i.duplikatkontroll = b.duplikatkontroll
            WHERE i.republisert IS NULL AND i.timeout < :timeout AND b.løsning IS NOT NULL""".trimMargin()
            .listQuery(mapOf("timeout" to LocalDateTime.now()))
            { row -> SendeklarInntektsmelding(
                row.string("fnr"),
                row.string("orgnummer"),
                Melding.les("inntektsmelding", row.string("data")) as Melding.Inntektsmelding,
                objectMapper.readTree(row.string("løsning")),
                row.localDateTime("mottatt")
            ) }
    }

    fun tellInntektsmeldinger(fnr: String, orgnummer: String, tattImotEtter: LocalDateTime): Int {
        return """SELECT COUNT (1)
            FROM inntektsmelding 
            WHERE mottatt >= :tattImotEtter AND fnr = :fnr AND orgnummer = :orgnummer
        """.trimMargin().singleQuery(mapOf("tattImotEtter" to tattImotEtter, "fnr" to fnr, "orgnummer" to orgnummer))
        { row -> row.int("count") }!!
    }

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "mottatt" to mottatt,
                            "timeout" to ønsketPublisert,
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1

}
