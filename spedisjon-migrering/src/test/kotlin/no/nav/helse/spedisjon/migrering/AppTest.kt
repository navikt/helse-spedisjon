package no.nav.helse.spedisjon.migrering

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.intellij.lang.annotations.Language
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import java.time.Instant
import javax.sql.DataSource

val databaseContainer = DatabaseContainers.container("spedisjon-migrering", CleanupStrategy.tables("arbeidstabell,umigrert"))
class AppTest {
    @Test
    fun initial() = e2e {
        sessionOf(it).use {
            it.run(queryOf("SELECT 1").asExecute)
        }
    }

    @Test
    fun migrering() = e2e { source ->
        val spedisjonConfig = HikariConfig().apply {
            (source as HikariDataSource).copyStateTo(this)
        }
        val spreSubsumsjonConfig = HikariConfig().apply {
            (source as HikariDataSource).copyStateTo(this)
        }
        val testperson = "12345678911"
        sessionOf(source).use { session ->
            @Language("PostgreSQL")
            val stmt1 = """create table melding (id bigint generated always as identity, fnr text, type text, ekstern_dokument_id uuid default null, intern_dokument_id uuid default null, data jsonb);"""
            session.run(queryOf(stmt1).asExecute)
            val treffPåInternId = session.run(queryOf("insert into melding(fnr, type, data) values (?, ?, ?::jsonb) returning id", testperson, "ny_søknad", """{ "sykmeldingId": "8fc424c7-7ddc-495d-be71-54ddf5fa543d", "@id": "c47c9d1a-bc36-4118-98d3-2a8d49b837fe" }""").map { row ->
                row.long("id")
            }.asList).single()
            val treffPåEksternIdMenUlikMeldingtype = session.run(queryOf("insert into melding(fnr, type, data) values (?, ?, ?::jsonb) returning id", testperson, "ny_søknad", """{ "sykmeldingId": "ae0bad00-eac2-4263-bb6c-37ae1618e3c1", "@id": "1f76e034-3e6e-422c-a296-f43cde57e320" }""").map { row ->
                row.long("id")
            }.asList).single()
            val treffPåEksternIdOgSammeMeldingType = session.run(queryOf("insert into melding(fnr, type, data) values (?, ?, ?::jsonb) returning id", testperson, "ny_søknad", """{ "sykmeldingId": "8d8bc5cb-589e-4a87-9f26-50ccc6f483c7", "@id": "642edaa3-ed8f-4624-8970-43b9f80e2766" }""").map { row ->
                row.long("id")
            }.asList).single()
            val ingenTreff = session.run(queryOf("insert into melding(fnr, type, data) values (?, ?, ?::jsonb) returning id", testperson, "ny_søknad", """{ "sykmeldingId": "9a0faa73-9470-486f-a518-e4660980d36f", "@id": "4c63eeeb-cc98-405e-a994-2ae2eb5bc351" }""").map { row ->
                row.long("id")
            }.asList).single()

            @Language("PostgreSQL")
            val stmt2 = """create table hendelse_dokument_mapping (hendelse_id uuid, dokument_id uuid, hendelse_type text);"""
            session.run(queryOf(stmt2).asExecute)
            session.run(queryOf("insert into hendelse_dokument_mapping(hendelse_id, dokument_id, hendelse_type) values (?::uuid, ?::uuid, ?)", "c47c9d1a-bc36-4118-98d3-2a8d49b837fe", "8fc424c7-7ddc-495d-be71-54ddf5fa543d", "ny_søknad").asExecute)
            session.run(queryOf("insert into hendelse_dokument_mapping(hendelse_id, dokument_id, hendelse_type) values (?::uuid, ?::uuid, ?)", "80dc4bca-60c0-4314-8e2d-9d30b2b9edbb", "ae0bad00-eac2-4263-bb6c-37ae1618e3c1", "sendt_søknad_nav").asExecute)
            session.run(queryOf("insert into hendelse_dokument_mapping(hendelse_id, dokument_id, hendelse_type) values (?::uuid, ?::uuid, ?)", "01625a3a-65af-48ff-954a-5360448fd7b6", "8d8bc5cb-589e-4a87-9f26-50ccc6f483c7", "ny_søknad").asExecute)


            utførMigrering(source, spedisjonConfig, spreSubsumsjonConfig, spedisjonConfig, sjekkSpleis = false)

            val arbeidRow = sessionOf(source).use { session ->
                session.run(queryOf("select id,fnr,arbeid_startet,arbeid_ferdig from arbeidstabell").map {
                    Arbeidrad(
                        id = it.long("id"),
                        fnr = it.string("fnr"),
                        arbeidStartet = it.instantOrNull("arbeid_startet"),
                        arbeidFerdig = it.instantOrNull("arbeid_ferdig")
                    )
                }.asList)
            }

            assertEquals(1, arbeidRow.size)
            arbeidRow[0].also {
                assertEquals(1, it.id)
                assertEquals(testperson.padStart(11, '0'), it.fnr)
                assertNotNull(it.arbeidStartet)
                assertNotNull(it.arbeidFerdig)
            }

            assertEquals(InternDokumentId("8fc424c7-7ddc-495d-be71-54ddf5fa543d", "c47c9d1a-bc36-4118-98d3-2a8d49b837fe"), session.internDokumentId(treffPåInternId))
            assertEquals(InternDokumentId("ae0bad00-eac2-4263-bb6c-37ae1618e3c1", "1f76e034-3e6e-422c-a296-f43cde57e320"), session.internDokumentId(treffPåEksternIdMenUlikMeldingtype))
            assertEquals(InternDokumentId("8d8bc5cb-589e-4a87-9f26-50ccc6f483c7", "01625a3a-65af-48ff-954a-5360448fd7b6"), session.internDokumentId(treffPåEksternIdOgSammeMeldingType))
            assertEquals(InternDokumentId("9a0faa73-9470-486f-a518-e4660980d36f", "4c63eeeb-cc98-405e-a994-2ae2eb5bc351"), session.internDokumentId(ingenTreff))
        }
    }

    private fun Session.internDokumentId(meldingId: Long) =
        run(queryOf("select ekstern_dokument_id,intern_dokument_id from melding where id=?", meldingId).map { row ->
            InternDokumentId(
                eksternId = row.stringOrNull("ekstern_dokument_id"),
                internId = row.stringOrNull("intern_dokument_id")
            )
        }.asList).single()

    private data class InternDokumentId(val eksternId: String?, val internId: String?)

    private data class Arbeidrad(
        val id: Long,
        val fnr: String,
        val arbeidStartet: Instant?,
        val arbeidFerdig: Instant?
    )
    private fun e2e(testblokk: (DataSource) -> Unit) {
        val testDataSource = databaseContainer.nyTilkobling()
        try {
            testblokk(testDataSource.ds)
        } finally {
            databaseContainer.droppTilkobling(testDataSource)
        }
    }
}