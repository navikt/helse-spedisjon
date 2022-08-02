package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.queryOf
import kotliquery.sessionOf
import java.time.LocalDateTime
import javax.sql.DataSource

internal class BerikelseDao(private val dataSource: DataSource) {

    fun behovEtterspurt(fnr: String, duplikatkontroll: String, behov: List<String>, opprettet: LocalDateTime) =
        sessionOf(dataSource).use {
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

    fun behovBesvart(duplikatkontroll: String, løsning: JsonNode) = sessionOf(dataSource).use {
        it.run(
            queryOf("""
                UPDATE berikelse SET løsning = :losning::json
                WHERE duplikatkontroll = :duplikatkontroll AND løsning is null
            """, mapOf("losning" to løsning.toString(), "duplikatkontroll" to duplikatkontroll)).asUpdate)
    }

    fun ubesvarteBehov(opprettetFør: LocalDateTime): List<UbesvartBehov> =
        sessionOf(dataSource).use {
            it.run(
                queryOf("""SELECT fnr, duplikatkontroll, behov FROM berikelse WHERE løsning is null AND opprettet < :opprettetFor""",
                    mapOf("opprettetFor" to opprettetFør))
                    .map { row ->
                        UbesvartBehov(
                            fnr = row.string("fnr"),
                            duplikatkontroll = row.string("duplikatkontroll"),
                            behov = row.string("behov").split(", ")
                        )
                    }.asList)
        }
}

internal data class UbesvartBehov(val fnr: String, val duplikatkontroll: String, val behov: List<String>)