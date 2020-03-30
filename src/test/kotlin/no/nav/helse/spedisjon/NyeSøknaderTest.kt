package no.nav.helse.spedisjon

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.MessageProblems
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import java.sql.Connection
import java.time.LocalDateTime
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class NyeSøknaderTest {
    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection
    private lateinit var dataSource: DataSource
    private val testRapid = TestRapid()

    @Test
    internal fun `leser nye søknader`() {
        NyeSøknader(testRapid, MeldingDao(dataSource), object : ProblemsCollector {
            override fun add(type: String, problems: MessageProblems) {}
        })
        testRapid.sendTestMessage(
            """
{
    "id": "id",
    "fnr": "fnr",
    "aktorId": "aktorId",
    "arbeidsgiver": {
        "orgnummer": "1234"
    },
    "opprettet": "${LocalDateTime.now()}",
    "soknadsperioder": [],
    "status": "NY",
    "sykmeldingId": "id",
    "fom": "2020-01-01",
    "tom": "2020-01-01"
}"""
        )

        assertEquals(1, using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM melding").map { it.long(1) }.asSingle)
        })
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

        Flyway.configure().dataSource(dataSource).load().migrate()
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

    @AfterAll
    fun `stop postgres`() {
        postgresConnection.close()
        embeddedPostgres.close()
    }

}
