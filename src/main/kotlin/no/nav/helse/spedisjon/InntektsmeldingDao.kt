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

    fun leggInn(melding: Melding.Inntektsmelding): Boolean {
        log.info("legger inn ekstra info om inntektsmelding")
        return leggInnUtenDuplikat(melding).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    private fun leggInnUtenDuplikat(melding: Melding.Inntektsmelding) =
        """INSERT INTO inntektsmelding (fnr, orgnummer, mottatt, timeout, duplikatkontroll) VALUES (:fnr, :orgnummer, :mottatt, :timeout, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf(  "fnr" to melding.fødselsnummer(),
                            "orgnummer" to melding.orgnummer(),
                            "mottatt" to LocalDateTime.now(),
                            "timeout" to LocalDateTime.now().plusMinutes(5),
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1
}
