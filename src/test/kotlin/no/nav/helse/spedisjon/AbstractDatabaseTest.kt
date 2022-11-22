package no.nav.helse.spedisjon

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.containers.PostgreSQLContainer
import java.time.LocalDateTime
import javax.sql.DataSource

abstract class AbstractDatabaseTest {
    private val postgres = PostgreSQLContainer<Nothing>("postgres:13")
    protected lateinit var dataSource: DataSource

    protected companion object {
        const val FØDSELSNUMMER = "fnr"
        const val ORGNUMMER = "a1"
        const val AKTØR = "aktørId"
        val OPPRETTET_DATO: LocalDateTime = LocalDateTime.now()
    }

    @BeforeEach
    fun setup() {
        Flyway.configure()
            .dataSource(dataSource)
            .cleanDisabled(false)
            .load()
            .also { it.clean() }
            .migrate()
    }

    @BeforeAll
    fun `start postgres`() {
        postgres.start()
        dataSource = HikariDataSource(HikariConfig().apply {
            jdbcUrl = postgres.jdbcUrl
            username = postgres.username
            password = postgres.password
            maximumPoolSize = 3
            minimumIdle = 1
            initializationFailTimeout = 5000
            idleTimeout = 10001
            connectionTimeout = 1000
            maxLifetime = 30001
        })
    }

    @AfterAll
    fun `stop postgres`() {
        postgres.stop()
    }


    protected fun antallMeldinger(fnr: String = FØDSELSNUMMER) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM melding WHERE fnr = ?", fnr).map { row ->
                row.long(1)
            }.asSingle)
        }

}