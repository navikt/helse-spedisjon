package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import java.time.LocalDateTime
import javax.sql.DataSource

internal class BerikelseDao(private val dataSource: DataSource) {

    fun behovEtterspurt(fnr: String, duplikatkontroll: String, behov: List<String>, opprettet: LocalDateTime) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf("""
                INSERT INTO berikelse (fnr, duplikatkontroll, behov, opprettet) values(:fnr, :duplikatkontroll, :behov, :opprettet)
                ON CONFLICT(duplikatkontroll) DO NOTHING
            """, mapOf(
                    "fnr" to fnr,
                    "duplikatkontroll" to duplikatkontroll,
                    "behov" to behov.joinToString(),
                    "opprettet" to opprettet)).asUpdate)
        }

    fun behovBesvart(duplikatkontroll: String, løsning: JsonNode) = using(sessionOf(dataSource)) {
        it.run(
            queryOf("""
                UPDATE berikelse SET løsning = :losning::json
                WHERE duplikatkontroll = :duplikatkontroll AND løsning is null
            """, mapOf("losning" to løsning.toString(), "duplikatkontroll" to duplikatkontroll)).asUpdate)
    }

    fun ubesvarteBehov(opprettetFør: LocalDateTime): List<RetryMelding> =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf("""SELECT fnr, duplikatkontroll FROM berikelse WHERE løsning is null AND opprettet < :opprettetFor""",
                    mapOf("opprettetFor" to opprettetFør))
                    .map { row ->
                        RetryMelding(fnr = row.string("fnr"),
                            duplikatkontroll = row.string("duplikatkontroll"))
                    }.asList)
        }
}

internal data class RetryMelding(val fnr: String, val duplikatkontroll: String)