package no.nav.helse.spedisjon

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDateTime
import javax.sql.DataSource

internal class MeldingDao(private val dataSource: DataSource) {

    fun leggInn(melding: String, dato: LocalDateTime) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    "INSERT INTO  melding (data, opprettet) VALUES (to_json(?::json), ?)",
                    melding,
                    dato
                ).asExecute
            )
        }
}
