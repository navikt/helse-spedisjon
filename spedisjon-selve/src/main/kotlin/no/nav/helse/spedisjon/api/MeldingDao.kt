package no.nav.helse.spedisjon.api

import java.util.*
import javax.sql.DataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import no.nav.sykepenger.libs.logging.loggInfo
import org.intellij.lang.annotations.Language

internal class MeldingDao(
    private val dataSource: DataSource
) {
    fun hentMeldinger(internDokumentIder: List<UUID>): List<MeldingDto> {
        if (internDokumentIder.isEmpty()) return emptyList()
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val stmt = """
                select fnr,type,intern_dokument_id,ekstern_dokument_id,duplikatkontroll,data 
                from melding 
                where ${internDokumentIder.joinToString(separator = " OR ") { "intern_dokument_id = ?" }}
                union all
                select m.fnr,m.type,m.intern_dokument_id,m.ekstern_dokument_id,m.duplikatkontroll,m.data 
                from melding_alias ma
                inner join melding m on m.id = ma.melding_id
                where ${internDokumentIder.joinToString(separator = " OR ") { "ma.intern_dokument_id = ?" }}
            ;"""
            // gjentar listen to ganger
            val dokumentIder = (internDokumentIder + internDokumentIder)
            session.run(
                queryOf(stmt, *dokumentIder.toTypedArray())
                    .map { row ->
                        MeldingDto(
                            type = row.string("type"),
                            fnr = row.string("fnr"),
                            internDokumentId = row.uuid("intern_dokument_id"),
                            eksternDokumentId = row.uuid("ekstern_dokument_id"),
                            duplikatkontroll = row.string("duplikatkontroll"),
                            jsonBody = row.string("data")
                        )
                    }.asList
            )
        }
    }

    fun leggInn(meldingsdetaljer: NyMeldingDto): Resultat {
        loggInfo(
            "legger inn melding",
            "duplikatkontroll" to meldingsdetaljer.duplikatkontroll,
            "jsonBody" to meldingsdetaljer.jsonBody
        )
        return insertDokument(meldingsdetaljer).also { resultat ->
            if (resultat.utfall == Resultat.Utfall.HENTET_EKSISTERENDE) {
                loggInfo(
                    "Meldingen er lagret fra før",
                    "duplikatkontroll" to meldingsdetaljer.duplikatkontroll,
                    "melding" to meldingsdetaljer.jsonBody
                )
            }
        }
    }

    /** inserter, eller henter, et dokument og returnerer intern ID i én atomisk operasjon **/
    data class Resultat(
        val utfall: Utfall,
        val internId: UUID
    ) {
        enum class Utfall {
            BLE_LAGRET_NÅ,
            HENTET_EKSISTERENDE
        }
    }

    private fun insertDokument(meldingsdetaljer: NyMeldingDto): Resultat =
        sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val insertStmt = """
            with verdier (fnr,type,ekstern_dokument_id,duplikatkontroll,data) as (
                values (:fnr, :type, cast(:eksternDokumentId as uuid), :duplikatkontroll, cast(:data as jsonb))
            ), inserted as (
                insert into melding(fnr,type,ekstern_dokument_id,duplikatkontroll,data)
                select fnr,type,ekstern_dokument_id,duplikatkontroll,data
                from verdier
                on conflict (duplikatkontroll) do nothing
                returning intern_dokument_id
            )
            select intern_dokument_id, true as ble_lagret_nå from inserted
            union all
            select m.intern_dokument_id, false as ble_lagret_nå
            from melding m
            where m.duplikatkontroll = :duplikatkontroll
            and not exists (select 1 from inserted);
        """
            session
                .run(
                    queryOf(
                        insertStmt,
                        mapOf(
                            "fnr" to meldingsdetaljer.fnr,
                            "type" to meldingsdetaljer.type,
                            "eksternDokumentId" to meldingsdetaljer.eksternDokumentId,
                            "duplikatkontroll" to meldingsdetaljer.duplikatkontroll,
                            "data" to meldingsdetaljer.jsonBody
                        )
                    ).map { row ->
                        Resultat(
                            utfall = if (row.boolean("ble_lagret_nå")) Resultat.Utfall.BLE_LAGRET_NÅ else Resultat.Utfall.HENTET_EKSISTERENDE,
                            internId = row.uuid("intern_dokument_id")
                        )
                    }.asList
                ).single()
        }
}

data class NyMeldingDto(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)

data class MeldingDto(
    val type: String,
    val fnr: String,
    val internDokumentId: UUID,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)
