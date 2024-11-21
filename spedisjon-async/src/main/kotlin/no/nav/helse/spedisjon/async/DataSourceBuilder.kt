package no.nav.helse.spedisjon.async

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

internal class DataSourceBuilder(env: Map<String, String>) {

    private val baseConnectionConfig = HikariConfig().apply {
        jdbcUrl = cloudSqlProxyConnectionString(env.getValue("DATABASE_HOST"), env.getValue("DATABASE_PORT").toInt(), env.getValue("DATABASE_DATABASE"))
        username = env.getValue("DATABASE_USERNAME")
        password = env.getValue("DATABASE_PASSWORD")
    }

    private val migrationConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        maximumPoolSize = 2
    }
    private val appConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        maximumPoolSize = 2
    }

    val dataSource by lazy { HikariDataSource(appConfig) }

    internal fun migrate() {
        logger.info("Migrerer database")
        HikariDataSource(migrationConfig).use {
            Flyway.configure()
                .dataSource(it)
                .lockRetryCount(-1)
                .load()
                .migrate()
        }
        logger.info("Migrering ferdig!")
    }

    private companion object {
        private val logger = LoggerFactory.getLogger(DataSourceBuilder::class.java)

        private fun cloudSqlProxyConnectionString(databaseHost: String, databasePort: Int, databaseName: String): String {
            return String.format("jdbc:postgresql://%s:%d/%s", databaseHost, databasePort, databaseName)
        }
    }
}
