package no.nav.helse.spedisjon.migrering

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
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
        val testperson = "fnr123"
        sessionOf(source).use { session ->
            @Language("PostgreSQL")
            val stmt = """create table melding (fnr text);"""
            session.run(queryOf(stmt).asExecute)
            session.run(queryOf("insert into melding(fnr) values (?)", testperson).asExecute)
        }
        utførMigrering(source, spedisjonConfig)

        val arbeidRow = sessionOf(source).use { session ->
            session.run(queryOf("select id,fnr,arbeid_startet,arbeid_ferdig from arbeidstabell").map {
                Arbeidrad(
                    id = it.long("id"),
                    fnr = it.string("fnr"),
                    arbeidStartet = it.instant("arbeid_startet"),
                    arbeidFerdig = it.instant("arbeid_ferdig")
                )
            }.asList)
        }.single()

        assertEquals(1, arbeidRow.id)
        assertEquals(testperson.padStart(11, '0'), arbeidRow.fnr)
        assertNotNull(arbeidRow.arbeidStartet)
        assertNotNull(arbeidRow.arbeidFerdig)
    }

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