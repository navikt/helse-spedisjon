package no.nav.helse.spedisjon

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import javax.sql.DataSource

internal class MeldingDao(private val dataSource: DataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
    }

    fun leggInn(melding: String, dato: LocalDateTime) {
        log.info("legger inn melding dato=$dato, melding=$melding")
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    "INSERT INTO melding (data, opprettet) VALUES (?::json, ?)",
                    melding,
                    dato
                ).asExecute
            )
        }
    }
}
