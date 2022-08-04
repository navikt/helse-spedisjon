package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import java.time.LocalDateTime
import javax.sql.DataSource

internal class BerikelseDao(dataSource: DataSource) : AbstractDao(dataSource) {

    fun behovEtterspurt(fnr: String, duplikatkontroll: String, behov: List<String>, opprettet: LocalDateTime) {
        """INSERT INTO berikelse (fnr, duplikatkontroll, behov, opprettet) values(:fnr, :duplikatkontroll, :behov, :opprettet)
                ON CONFLICT(duplikatkontroll) DO NOTHING"""
            .update(mapOf(
                "fnr" to fnr,
                "duplikatkontroll" to duplikatkontroll,
                "behov" to behov.joinToString(),
                "opprettet" to opprettet))
    }

    fun behovBesvart(duplikatkontroll: String, løsning: JsonNode) =
        """UPDATE berikelse SET løsning = :losning::json WHERE duplikatkontroll = :duplikatkontroll AND løsning is null"""
            .update(mapOf("losning" to løsning.toString(), "duplikatkontroll" to duplikatkontroll))

    fun ubesvarteBehov(opprettetFør: LocalDateTime): List<UbesvartBehov> =
         """SELECT fnr, duplikatkontroll, behov FROM berikelse WHERE løsning is null AND opprettet < :opprettetFor"""
             .listQuery(mapOf("opprettetFor" to opprettetFør))
                { row -> UbesvartBehov( fnr = row.string("fnr"),
                                        duplikatkontroll = row.string("duplikatkontroll"),
                                        behov = row.string("behov").split(", ")) }

    fun behovErBesvart(duplikatkontroll: String) =
            """ SELECT 1 FROM berikelse WHERE duplikatkontroll = :duplikatkontroll AND løsning is not null """
                .listQuery(mapOf("duplikatkontroll" to duplikatkontroll))
                { row -> row}.isNotEmpty()
}

internal data class UbesvartBehov(val fnr: String, val duplikatkontroll: String, val behov: List<String>)