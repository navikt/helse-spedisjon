package no.nav.helse.spedisjon

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import no.nav.helse.spedisjon.Melding.Companion.sha512
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.PostgreSQLContainer
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant
import java.util.*

internal class NyDuplikatkontrollTest {

    private val postgres = PostgreSQLContainer<Nothing>("postgres:13")
    private lateinit var hikariConfig: HikariConfig


    @BeforeEach
    fun `start postgres`() {
        postgres.start()
        hikariConfig = HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            maximumPoolSize = 3
            initializationFailTimeout = 5000
            minimumIdle = 1
            idleTimeout = 10001
            connectionTimeout = 1000
            maxLifetime = 30001
        }
    }

    @AfterEach
    fun `stop postgres`() {
        postgres.stop()
    }

    private fun PreparedStatement.leggTilMelding(id: UUID) {
        setString(1, """{"id": "$id", "status": "NY"}""")
        setString(2, "11111111111")
        setTimestamp(3, Timestamp.from(Instant.now()))
        setString(4, "ny_søknad")
        setString(5, "${UUID.randomUUID()}")
        addBatch()
    }

    private fun UUID.duplikatkontroll() = "${this}NY".sha512()

    @Test
    fun `legger til ny duplikatkontroll og markerer duplikate meldinger for sletting`() {
        val hikariDataSource = HikariDataSource(hikariConfig)
        val flyway = Flyway.configure().dataSource(hikariDataSource)

        flyway.target(MigrationVersion.fromVersion("2")).load().migrate()

        val antallDuplikater = 10
        val forventedeDuplikatkontroller = mutableListOf<String>()
        val forventedeDuplikateDuplikatkontroller = mutableListOf<String>()

        hikariDataSource.connection.prepareStatement("INSERT INTO melding(data, fnr, opprettet, type, duplikatkontroll) VALUES(cast(? as JSONB),?,?,?,?)").use { insertStatement ->
            lateinit var forrige : UUID
            repeat(100_000 - antallDuplikater) {
                forrige = UUID.randomUUID()
                forventedeDuplikatkontroller.add(forrige.duplikatkontroll())
                insertStatement.leggTilMelding(forrige)
            }

            repeat(antallDuplikater) {
                forventedeDuplikateDuplikatkontroller.add(forrige.duplikatkontroll())
                insertStatement.leggTilMelding(forrige)
            }

            insertStatement.executeBatch()
        }

        flyway.target(MigrationVersion.fromVersion("6")).load().migrate()

        hikariDataSource.connection.createStatement().executeQuery("SELECT tmp_duplikatkontroll FROM melding WHERE tmp_slett=false").use { resultSet ->
            while (resultSet.next()) {
                assertTrue(forventedeDuplikatkontroller.remove(resultSet.getString("tmp_duplikatkontroll")))
            }
        }
        assertTrue(forventedeDuplikatkontroller.isEmpty())

        hikariDataSource.connection.createStatement().executeQuery("SELECT tmp_duplikatkontroll FROM melding WHERE tmp_slett=true").use { resultSet ->
            while (resultSet.next()) {
                assertTrue(forventedeDuplikateDuplikatkontroller.remove(resultSet.getString("tmp_duplikatkontroll")))
            }
        }
        assertTrue(forventedeDuplikateDuplikatkontroller.isEmpty())
    }
}
