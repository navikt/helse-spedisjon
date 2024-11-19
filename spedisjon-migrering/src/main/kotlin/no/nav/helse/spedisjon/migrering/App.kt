package no.nav.helse.spedisjon.migrering

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.io.path.Path
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

val log: Logger = LoggerFactory.getLogger(::main::class.java)

private fun connectionConfigFromMountPath(instanceId: String, mountPath: String): HikariConfig {
    val secretsPath = Path(mountPath).listDirectoryEntries()
    /* søker etter filer som slutter på det vi forventer, fordi miljøvariabelene/filnavnet kan ha
        en custom prefiks som hver app har bestemt selv
     */

    log.info("fant følgende filer under $mountPath: ${secretsPath.joinToString { it.name } }")

    val databaseName = secretsPath.first { it.name.endsWith("_DATABASE") }.readText()
    val databaseUsername = secretsPath.first { it.name.endsWith("_USERNAME") }.readText()
    val databasePassword = secretsPath.first { it.name.endsWith("_PASSWORD") }.readText()

    return HikariConfig().apply {
        jdbcUrl = cloudSqlConnectionString(
            gcpProjectId = System.getenv("GCP_PROJECT_ID"),
            databaseInstance = instanceId,
            databaseName = databaseName,
            databaseRegion = System.getenv("GCP_SQL_REGION")
        )
        username = databaseUsername
        password = databasePassword
    }
}
private fun cloudSqlConnectionString(gcpProjectId: String, databaseInstance: String, databaseName: String, databaseRegion: String): String {
    return String.format(
        "jdbc:postgresql:///%s?%s&%s",
        databaseName,
        "cloudSqlInstance=$gcpProjectId:$databaseRegion:$databaseInstance",
        "socketFactory=com.google.cloud.sql.postgres.SocketFactory"
    )
}

private fun cloudSqlProxyConnectionString(databaseName: String) =
    String.format("jdbc:postgresql://%s:%s/%s", "127.0.0.1", "5432", databaseName)

private val baseConnectionConfig = HikariConfig().apply {
    jdbcUrl = cloudSqlProxyConnectionString(System.getenv("DATABASE_DATABASE"))
    username = System.getenv("DATABASE_USERNAME")
    password = System.getenv("DATABASE_PASSWORD")
}

private val flywayMigrationConfig = HikariConfig().apply {
    baseConnectionConfig.copyStateTo(this)
    maximumPoolSize = 2
    poolName = "flyway-migration"
    maxLifetime = Duration.ofMinutes(30).toMillis()
    idleTimeout = Duration.ofSeconds(10).toMillis()
    connectionTimeout = Duration.ofMinutes(30).toMillis()
    initializationFailTimeout = Duration.ofMinutes(30).toMillis()
}
private val appConfig = HikariConfig().apply {
    baseConnectionConfig.copyStateTo(this)
    poolName = "spedisjon-migrering"
    maximumPoolSize = 1
}

fun main() {
    try {
        workMain()
    } catch (err: Exception) {
        log.error("Alvorlig feil: ${err.message}. Jobben stopper, men venter i 5 minutter.", err)
        Thread.sleep(5.minutes.toJavaDuration())
    }
}

private fun migrateDatabase(connectionConfig: HikariConfig) {
    HikariDataSource(connectionConfig).use { dataSource ->
        val flyway = Flyway.configure()
            .dataSource(dataSource)
            .validateMigrationNaming(true)
            .cleanDisabled(false)
            .load()
        flyway.migrate()
    }
}

private fun workMain() {
    log.info("Tester datasourcer")

    val spedisjonConfig = HikariConfig().apply {
        connectionConfigFromMountPath(System.getenv("SPEDISJON_INSTANCE"), "/var/run/secrets/sql/spedisjon")
            .copyStateTo(this)
        poolName = "spedisjon"
        maximumPoolSize = 1
    }
    val spreSubsumsjon = HikariConfig().apply {
        connectionConfigFromMountPath(System.getenv("SPRE_SUBSUMSJON_INSTANCE"), "/var/run/secrets/sql/spre_subsumsjon")
            .copyStateTo(this)
        poolName = "spre-subsumsjon"
        maximumPoolSize = 1
    }

    listOf(flywayMigrationConfig, appConfig, spedisjonConfig, spreSubsumsjon).forEach {
        HikariDataSource(it).apply {
            log.info("${it.poolName} OK!")
        }
    }
}
