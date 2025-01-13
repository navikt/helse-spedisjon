package no.nav.helse.spedisjon.async

import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import java.util.UUID
import javax.sql.DataSource

class EkspederingDao(private val dataSourceProvider: () -> DataSource) {
    private val dataSource by lazy(dataSourceProvider)

    fun meldingEkspedert(internId: UUID): Boolean {
        return sessionOf(dataSource).use { session ->
            @Language("PostgreSQL")
            val stmt = """insert into ekspedering(intern_dokument_id, ekspedert) 
                values (?, now()) 
                on conflict (intern_dokument_id) do nothing 
                returning id"""
            session.run(queryOf(stmt, internId).map { row ->
                row.long("id")
            }.asSingle) != null
        }
    }
}
