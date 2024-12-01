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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
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
    @Disabled
    fun migrering() = e2e { source ->
        val spleisConfig = HikariConfig().apply {
            (source as HikariDataSource).copyStateTo(this)
        }
        val spedisjonConfig = HikariConfig().apply {
            (source as HikariDataSource).copyStateTo(this)
        }
        val spedisjonAsyncConfig = HikariConfig().apply {
            (source as HikariDataSource).copyStateTo(this)
        }
        val testperson = "12345678911"
        sessionOf(source).use { session ->
            @Language("PostgreSQL")
            val stmt1 = """create table melding (id bigint generated always as identity, fnr text, intern_dokument_id uuid default null, opprettet timestamp default now());"""
            session.run(queryOf(stmt1).asExecute)
            val hendelse1 = UUID.randomUUID()
            val enHendelse = session.run(queryOf("insert into melding(fnr, intern_dokument_id) values (?, ?) returning id", testperson, hendelse1).map { row ->
                row.long("id")
            }.asList).single()

            @Language("PostgreSQL")
            val stmt2 = """create table ekspedering(intern_dokument_id uuid unique, ekspedert timestamptz);"""
            session.run(queryOf(stmt2).asExecute)

            utførMigrering(source, spleisConfig, spedisjonConfig, spedisjonAsyncConfig)

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

            assertTrue(session.internDokumentId(hendelse1))
        }
    }

    private fun Session.internDokumentId(internId: UUID) =
        run(queryOf("select exists (select 1 from ekspedering where intern_dokument_id=?)", internId).map { row ->
            row.boolean(1)
        }.asList).single()

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