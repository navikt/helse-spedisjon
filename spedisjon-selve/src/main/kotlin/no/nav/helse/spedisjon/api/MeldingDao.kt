package no.nav.helse.spedisjon.api

import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

internal class MeldingDao(private val dataSource: DataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
    }

    fun hentMeldinger(internDokumentIder: List<UUID>): List<MeldingDto> {
        if (internDokumentIder.isEmpty()) return emptyList()
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val stmt = """
                select fnr,type,intern_dokument_id,ekstern_dokument_id,duplikatkontroll,data,opprettet 
                from melding 
                where 
                    ${internDokumentIder.joinToString(separator = " OR ") {
                        "intern_dokument_id = ?"
                    }}
            ;"""
            session.run(queryOf(stmt, *internDokumentIder.toTypedArray()).map { row ->
                MeldingDto(
                    type = row.string("type"),
                    fnr = row.string("fnr"),
                    internDokumentId = row.uuid("intern_dokument_id"),
                    eksternDokumentId = row.uuid("ekstern_dokument_id"),
                    rapportertDato = row.localDateTime("opprettet"),
                    duplikatkontroll = row.string("duplikatkontroll"),
                    jsonBody = row.string("data")
                )
            }.asList)
        }
    }

    fun leggInn(meldingsdetaljer: NyMeldingDto): Resultat {
        log.info("legger inn melding, rapportertDato=${meldingsdetaljer.rapportertDato},duplikatkontroll=${meldingsdetaljer.duplikatkontroll}\n${meldingsdetaljer.jsonBody}")
        return insertDokument(meldingsdetaljer).also { resultat ->
            if (resultat.utfall == Resultat.Utfall.HENTET_EKSISTERENDE) {
                log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", meldingsdetaljer.duplikatkontroll), meldingsdetaljer.jsonBody)
            }
        }
    }

    /** inserter, eller henter, et dokument og returnerer intern ID i én atomisk operasjon **/
    data class Resultat(
        val utfall: Utfall,
        val internId: UUID,
    ) {
        enum class Utfall {
            SATT_INN_NY,
            HENTET_EKSISTERENDE
        }
    }
    private fun insertDokument(meldingsdetaljer: NyMeldingDto): Resultat {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val insertStmt = """
            with verdier as (
                select fnr,type,ekstern_dokument_id,duplikatkontroll,data,opprettet
                from (values (:fnr, :type, cast(:eksternDokumentId as uuid), :duplikatkontroll, cast(:data as jsonb), cast(:opprettet as timestamp))) v(fnr, type, ekstern_dokument_id, duplikatkontroll, data, opprettet)
            ), ins as (
                insert into melding(fnr,type,ekstern_dokument_id,duplikatkontroll,data,opprettet)
                select fnr,type,ekstern_dokument_id,duplikatkontroll,data,opprettet
                from verdier
                on conflict (duplikatkontroll) do nothing
                returning intern_dokument_id
            )
            select 'i' as kilde, intern_dokument_id
            from ins
            union all
            select 's' as kilde, m.intern_dokument_id
            from verdier v
            join melding m
            on v.duplikatkontroll = m.duplikatkontroll;
        """
            session.run(queryOf(insertStmt, mapOf(
                "fnr" to meldingsdetaljer.fnr,
                "type" to meldingsdetaljer.type,
                "eksternDokumentId" to meldingsdetaljer.eksternDokumentId,
                "duplikatkontroll" to meldingsdetaljer.duplikatkontroll,
                "data" to meldingsdetaljer.jsonBody,
                "opprettet" to meldingsdetaljer.rapportertDato
            )).map { row ->
                Resultat(
                    utfall = when (val kilde = row.string("kilde")) {
                        "i" -> Resultat.Utfall.SATT_INN_NY
                        "s" -> Resultat.Utfall.HENTET_EKSISTERENDE
                        else -> error("Ukjent kilde: $kilde")
                    },
                    internId = row.uuid("intern_dokument_id")
                )
            }.asList).single()
        }
    }
}

data class NyMeldingDto(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)

data class MeldingDto(
    val type: String,
    val fnr: String,
    val internDokumentId: UUID,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)