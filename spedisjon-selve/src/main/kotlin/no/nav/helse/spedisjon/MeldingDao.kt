package no.nav.helse.spedisjon

import kotliquery.queryOf
import kotliquery.sessionOf
import net.logstash.logback.argument.StructuredArguments.keyValue
import org.intellij.lang.annotations.Language
import org.slf4j.LoggerFactory
import java.util.*
import javax.sql.DataSource

internal class MeldingDao(dataSource: DataSource): AbstractDao(dataSource) {

    private companion object {
        private val log = LoggerFactory.getLogger("tjenestekall")
    }

    fun leggInn(meldingsdetaljer: Meldingsdetaljer): UUID? {
        log.info("legger inn melding, rapportertDato=${meldingsdetaljer.rapportertDato},duplikatkontroll=${meldingsdetaljer.duplikatkontroll}\n${meldingsdetaljer.jsonBody}")
        return insertDokument(meldingsdetaljer)
            .takeIf { it.utfall == Resultat.Utfall.SATT_INN_NY }
            ?.internId
            ?: null.also {
                log.info("Duplikat melding: {} melding={}", keyValue("duplikatkontroll", meldingsdetaljer.duplikatkontroll), meldingsdetaljer.jsonBody)
            }
    }

    /** inserter, eller henter, et dokument og returnerer intern ID i én atomisk operasjon **/
    private data class Resultat(
        val utfall: Utfall,
        val internId: UUID,
    ) {
        enum class Utfall {
            SATT_INN_NY,
            HENTET_EKSISTERENDE
        }
    }
    private fun insertDokument(meldingsdetaljer: Meldingsdetaljer): Resultat {
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
            select 's' as kilde, coalesce(m.intern_dokument_id,cast(m.data->>'@id' as uuid)) as intern_dokument_id
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
