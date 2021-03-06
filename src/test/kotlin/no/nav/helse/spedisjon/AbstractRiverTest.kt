package no.nav.helse.spedisjon

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.*
import java.sql.Connection
import java.time.LocalDateTime
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal abstract class AbstractRiverTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection
    private lateinit var dataSource: DataSource
    protected val testRapid = TestRapid()

    protected companion object {
        const val FØDSELSNUMMER = "fnr"
        const val AKTØR = "aktørId"
        val OPPRETTET_DATO: LocalDateTime = LocalDateTime.now()
    }

    protected abstract fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource)

    @BeforeEach
    fun setup() {
        Flyway.configure()
            .dataSource(dataSource)
            .load()
            .also { it.clean() }
            .migrate()

        createRiver(testRapid, dataSource)
    }

    @AfterEach
    internal fun `clear messages`() {
        testRapid.reset()
    }

    @BeforeAll
    fun `start postgres`() {
        embeddedPostgres = EmbeddedPostgres.builder().start()
        postgresConnection = embeddedPostgres.postgresDatabase.connection
        dataSource = HikariDataSource(createHikariConfig(embeddedPostgres.getJdbcUrl("postgres", "postgres")))
    }

    @AfterAll
    fun `stop postgres`() {
        postgresConnection.close()
        embeddedPostgres.close()
    }

    protected fun antallMeldinger(fnr: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM melding WHERE fnr = ?", fnr).map {
                it.long(1)
            }.asSingle)
        }

    private fun createHikariConfig(jdbcUrl: String) =
        HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            maximumPoolSize = 3
            minimumIdle = 1
            idleTimeout = 10001
            connectionTimeout = 1000
            maxLifetime = 30001
        }

}
