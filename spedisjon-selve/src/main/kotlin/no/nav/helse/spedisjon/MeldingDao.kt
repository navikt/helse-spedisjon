package no.nav.helse.spedisjon

import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal class MeldingDao(dataSource: DataSource): AbstractDao(dataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
    }

    fun leggInn(melding: Melding): Boolean {
        log.info("legger inn melding, rapportertDato=${melding.rapportertDato()}\n${melding.json()}")
        return leggInnUtenDuplikat(melding).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    private fun leggInnUtenDuplikat(melding: Melding) =
        """INSERT INTO melding (type, fnr, data, opprettet, duplikatkontroll) VALUES (:type, :fnr, :json::json, :rapportert, :duplikatkontroll) ON CONFLICT(duplikatkontroll) do nothing"""
            .update(mapOf( "type" to melding.type,
                            "fnr" to melding.fødselsnummer(),
                            "json" to melding.json(),
                            "rapportert" to melding.rapportertDato(),
                            "duplikatkontroll" to melding.duplikatkontroll())) == 1
}
