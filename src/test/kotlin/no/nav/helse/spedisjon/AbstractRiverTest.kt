package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.*
import org.testcontainers.containers.PostgreSQLContainer
import java.time.LocalDateTime
import javax.sql.DataSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal abstract class AbstractRiverTest {
    private val postgres = PostgreSQLContainer<Nothing>("postgres:13")
    private lateinit var dataSource: DataSource
    protected val testRapid = TestRapid()

    protected companion object {
        private val objectMapper = jacksonObjectMapper()
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

    protected fun hentDuplikatkontroll(fnr: String = FØDSELSNUMMER): String? {
        return sessionOf(dataSource).use {
            it.run(
                queryOf(
                    """SELECT duplikatkontroll FROM melding WHERE fnr = ?""",
                    fnr
                ).map { row ->
                    row.string("duplikatkontroll")
                }.asSingle
            )
        }
    }

    protected fun String.json(block: (node: ObjectNode) -> Unit) : String {
        val node = objectMapper.readTree(this) as ObjectNode
        block(node)
        return node.toString()
    }
}
