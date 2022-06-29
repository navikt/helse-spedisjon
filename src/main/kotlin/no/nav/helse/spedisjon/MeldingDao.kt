package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.slf4j.LoggerFactory
import javax.sql.DataSource

internal class MeldingDao(private val dataSource: DataSource){

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
        private val objectMapper = jacksonObjectMapper()
    }

    fun leggInn(melding: Melding): Boolean {
        log.info("legger inn melding, rapportertDato=${melding.rapportertDato()}\n${melding.json()}")
        return leggInnUtenDuplikat(melding).also {
            if (!it) log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", melding.duplikatkontroll()), melding.json())
        }
    }

    fun hent(dupliatkontroll: String): Pair<String, JsonNode>? {
        return sessionOf(dataSource).use {
            it.run(
                queryOf(
                    """SELECT fnr, data FROM melding WHERE duplikatkontroll = ?""",
                    dupliatkontroll
                ).map { row ->
                    row.string("fnr") to objectMapper.readTree(row.string("data"))
                }.asSingle
            )
        }
    }

    private fun leggInnUtenDuplikat(melding: Melding) =
        using(sessionOf(dataSource)) {
            it.run(
                queryOf(
                    """INSERT INTO melding (type, fnr, data, opprettet, duplikatkontroll)
                    VALUES (?, ?, ?::json, ?, ?) ON CONFLICT(duplikatkontroll) do nothing""",
                    melding.type,
                    melding.fødselsnummer(),
                    melding.json(),
                    melding.rapportertDato(),
                    melding.duplikatkontroll()
                ).asUpdate
            )
        } == 1
}
