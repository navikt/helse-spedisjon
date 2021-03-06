package no.nav.helse.spedisjon

import com.opentable.db.postgres.embedded.EmbeddedPostgres
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.sql.Connection
import java.time.LocalDateTime

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DuplikateMeldingerTest {

    private lateinit var embeddedPostgres: EmbeddedPostgres
    private lateinit var postgresConnection: Connection

    private lateinit var hikariConfig: HikariConfig
    private lateinit var dataSource: HikariDataSource
    private lateinit var meldingDao: MeldingDao

    @BeforeAll
    fun `start postgres`() {
        embeddedPostgres = EmbeddedPostgres.builder()
            .start()

        postgresConnection = embeddedPostgres.postgresDatabase.connection

        hikariConfig = createHikariConfig(embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        dataSource = HikariDataSource(hikariConfig)
        runMigration(dataSource)

        meldingDao = MeldingDao(dataSource)
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

    private fun runMigration(dataSource: HikariDataSource) =
        Flyway.configure()
            .dataSource(dataSource)
            .load()
            .migrate()

    @Test
    fun `duplikat inntektsmelding slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "arkivreferanse": "1",
                    "arbeidstakerFnr": "123",
                    "mottattDato": "${LocalDateTime.now()}"
                }
            """, MessageProblems("")).apply {
            requireKey("arkivreferanse", "arbeidstakerFnr", "mottattDato")
        }

        assertTrue(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
        assertFalse(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
    }

    @Test
    fun `duplikat sendt søknad til nav slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "1",
                    "fnr": "123",
                    "sendtNav": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """, MessageProblems("")).apply {
            requireKey("id", "fnr", "sendtNav", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "1",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """, MessageProblems("")).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver (ettersending) slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "1",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "sendtNav": "${LocalDateTime.now().minusDays(1)}",
                    "status": "SENDT"
                }
            """, MessageProblems("")).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
    }

    @Test
    fun `duplikat ny søknad slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "1",
                    "fnr": "123",
                    "opprettet": "${LocalDateTime.now()}",
                    "status": "NY"
                }
            """, MessageProblems("")).apply {
            requireKey("id", "fnr", "opprettet", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.NySøknad(packet)))
        assertFalse(meldingDao.leggInn(Melding.NySøknad(packet)))
    }
}
