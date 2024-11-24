package no.nav.helse.spedisjon.api

import com.github.navikt.tbd_libs.naisful.postgres.ConnectionConfigFactory
import com.github.navikt.tbd_libs.naisful.postgres.defaultJdbcUrl
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

internal class DataSourceBuilder(private val meterRegistry: PrometheusMeterRegistry) {

    private val baseConnectionConfig = HikariConfig().apply {
        jdbcUrl = defaultJdbcUrl(ConnectionConfigFactory.Env(envVarPrefix = "DATABASE"))
        metricRegistry = meterRegistry
    }

    private val migrationConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        poolName = "flyway"
        maximumPoolSize = 2
    }
    private val appConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        poolName = "app"
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
    }
}
