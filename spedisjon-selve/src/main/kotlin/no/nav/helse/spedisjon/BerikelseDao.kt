package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import kotliquery.sessionOf
import org.slf4j.Logger
import java.time.Duration
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

    fun behovBesvart(duplikatkontroll: String, løsning: JsonNode, handling: () -> Unit) {
        sessionOf(dataSource).use {
            it.transaction { txSession ->
                check(1 == """UPDATE berikelse SET løsning = :losning::json WHERE duplikatkontroll = :duplikatkontroll AND løsning is null"""
                    .update(txSession, mapOf("losning" to løsning.toString(), "duplikatkontroll" to duplikatkontroll))) {
                    "forventer å oppdatere berikelse, men ingen rad fantes!"
                }
                handling()
            }
        }
    }

    fun ubesvarteBehov(opprettetFør: LocalDateTime): List<UbesvartBehov> =
         """SELECT fnr, duplikatkontroll, behov, opprettet FROM berikelse WHERE løsning is null AND opprettet < :opprettetFor"""
             .listQuery(mapOf("opprettetFor" to opprettetFør))
                { row -> UbesvartBehov( fnr = row.string("fnr"),
                                        duplikatkontroll = row.string("duplikatkontroll"),
                                        behov = row.string("behov").split(", "),
                                        opprettet = row.localDateTime("opprettet")) }
    fun behovErBesvart(duplikatkontroll: String) =
            """SELECT 1 FROM berikelse WHERE duplikatkontroll = :duplikatkontroll AND løsning is not null"""
                .listQuery(mapOf("duplikatkontroll" to duplikatkontroll))
                { row -> row }.isNotEmpty()

    fun behovErEtterspurt(duplikatkontroll: String) =
            """SELECT 1 FROM berikelse WHERE duplikatkontroll = :duplikatkontroll"""
                .listQuery(mapOf("duplikatkontroll" to duplikatkontroll))
                { row -> row }.isNotEmpty()
}

internal data class UbesvartBehov(
    val fnr: String,
    val duplikatkontroll: String,
    val behov: List<String>,
    private val opprettet: LocalDateTime) {
    private val sidenOpprettet = Duration.between(opprettet, LocalDateTime.now())
    val gammelt = sidenOpprettet > treTimer
    fun logg(logger: Logger) {
        if (gammelt) logger.error("Sender ut nytt behov for gammel melding med duplikatkontroll=$duplikatkontroll. Meldingen kom inn $opprettet ($sidenOpprettet siden). Denne bør undersøkes nærmere!")
        else logger.info("Sender ut nytt behov for melding med duplikatkontroll=$duplikatkontroll. Meldingen kom inn $opprettet ($sidenOpprettet siden)")
    }
    private companion object {
        private val treTimer = Duration.ofHours(3)
    }
}