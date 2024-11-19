package no.nav.helse.spedisjon.migrering

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import kotliquery.queryOf
import kotliquery.sessionOf
import org.junit.jupiter.api.Test
import javax.sql.DataSource

val databaseContainer = DatabaseContainers.container("spedisjon-migrering", CleanupStrategy.tables("arbeidstabell,umigrert"))
class AppTest {
    @Test
    fun initial() = e2e {
        sessionOf(it).use {
            it.run(queryOf("SELECT 1").asExecute)
        }
    }

    private fun e2e(testblokk: (DataSource) -> Unit) {
        val testDataSource = databaseContainer.nyTilkobling()
        try {
            testblokk(testDataSource.ds)
        } finally {
            databaseContainer.droppTilkobling(testDataSource)
        }
    }
}