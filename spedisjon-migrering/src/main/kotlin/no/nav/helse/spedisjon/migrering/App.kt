package no.nav.helse.spedisjon.migrering

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import org.flywaydb.core.Flyway
import org.intellij.lang.annotations.Language
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.sql.DataSource
import kotlin.io.path.Path
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.measureTime
import kotlin.time.toJavaDuration

val log: Logger = LoggerFactory.getLogger("no.nav.helse.spedisjon.migrering.App")

fun main() {
    try {
        workMain()
        log.info("Appen er ferdig, men venter i 1 minutt med å avslutte.")
        Thread.sleep(1.minutes.toJavaDuration())
    } catch (err: Exception) {
        log.error("Alvorlig feil: ${err.message}. Jobben stopper, men venter i 5 minutter.", err)
        Thread.sleep(5.minutes.toJavaDuration())
    }
}

private fun workMain() {
    val baseConnectionConfig = HikariConfig().apply {
        jdbcUrl = cloudSqlConnectionString(
            gcpProjectId = System.getenv("GCP_PROJECT_ID"),
            databaseInstance = System.getenv("SPEDISJON_MIGRATE_INSTANCE"),
            databaseName = System.getenv("DATABASE_DATABASE"),
            databaseRegion = System.getenv("GCP_SQL_REGION")
        )
        username = System.getenv("DATABASE_USERNAME")
        password = System.getenv("DATABASE_PASSWORD")
    }

    val flywayMigrationConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        maximumPoolSize = 2
        poolName = "flyway-migration"
        initializationFailTimeout = -1
    }
    val appConfig = HikariConfig().apply {
        baseConnectionConfig.copyStateTo(this)
        poolName = "spedisjon-migrering"
        maximumPoolSize = 1
    }

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

    testTilkoblinger(flywayMigrationConfig, appConfig, spedisjonConfig, spreSubsumsjon)
    migrateDatabase(flywayMigrationConfig)

    HikariDataSource(appConfig).use { dataSource ->
        utførMigrering(dataSource, spedisjonConfig)
    }
}

fun utførMigrering(dataSource: DataSource, spedisjonConfig: HikariConfig) {
    sessionOf(dataSource).use { session ->
        klargjørEllerVentPåTilgjengeligArbeid(session) {
            log.info("Henter personer fra spedisjon og forbereder arbeidstabell")

            measureTime {
                HikariDataSource(spedisjonConfig).use { spedisjonDataSource ->
                    sessionOf(spedisjonDataSource).use { spedisjonSession ->
                        val limit = 50_000
                        @Language("PostgreSQL")
                        val stmt = "select fnr from melding group by fnr order by fnr limit $limit offset ?"
                        var page = 0
                        do {
                            val offset = limit * page
                            page += 1

                            log.info("[page $page] henter personer")
                            val personer = spedisjonSession.run(queryOf(stmt, offset).map { row ->
                                row.string("fnr").padStart(11, '0')
                            }.asList)

                            log.info("[page $page] inserter i arbeidstabell")

                            if (personer.isNotEmpty()) {
                                @Language("PostgreSQL")
                                val query = """INSERT INTO arbeidstabell (fnr) VALUES ${personer.joinToString { "(?)" }}"""
                                session.run(queryOf(query, *personer.toTypedArray()).asExecute)
                            }

                            log.info("[page $page] complete")
                        } while (personer.size >= limit)
                    }
                }
            }.also {
                log.info("Brukte ${it.inWholeSeconds} sekunder på å fylle arbeidstabellen")
            }
        }
        utførArbeid(session) { arbeid ->
            log.info("Arbeider på ${arbeid.id}")
        }
    }
}

data class Arbeid(val id: Long, val fnr: String)

private fun utførArbeid(session: Session, arbeider: (arbeid: Arbeid) -> Unit) {
    do {
        log.info("Forsøker å hente arbeid")
        val arbeidsliste = hentArbeid(session)
            .also {
                if (it.isNotEmpty()) log.info("Fikk ${it.size} stk")
            }
            .onEach {
                try {
                    arbeider(it)
                } finally {
                    arbeidFullført(session, it)
                }
            }
    } while (arbeidsliste.isNotEmpty())
    log.info("Fant ikke noe arbeid, avslutter")
}

private fun fåLås(session: Session): Boolean {
    // oppretter en lås som varer ut levetiden til sesjonen.
    // returnerer umiddelbart med true/false avhengig om vi fikk låsen eller ikke
    @Language("PostgreSQL")
    val query = "SELECT pg_try_advisory_lock(1337)"
    return session.run(queryOf(query).map { it.boolean(1) }.asSingle)!!
}

private fun hentArbeid(session: Session, size: Int = 500): List<Arbeid> {
    @Language("PostgreSQL")
    val query = """
    select id,fnr from arbeidstabell where arbeid_startet IS NULL limit $size for update skip locked; 
    """
    @Language("PostgreSQL")
    val oppdater = "update arbeidstabell set arbeid_startet=now() where id IN(%s)"
    return session.transaction { txSession ->
        txSession.run(queryOf(query).map {
            Arbeid(
                id = it.long("id"),
                fnr = it.string("fnr").padStart(11, '0')
            )
        }.asList).also { personer ->
            if (personer.isNotEmpty()) {
                txSession.run(queryOf(String.format(oppdater, personer.joinToString { "?" }), *personer.map { it.id }.toTypedArray()).asUpdate)
            }
        }
    }
}
private fun arbeidFullført(session: Session, arbeid: Arbeid) {
    @Language("PostgreSQL")
    val query = "update arbeidstabell set arbeid_ferdig=now() where id=?"
    session.run(queryOf(query, arbeid.id).asUpdate)
}
private fun arbeidFinnes(session: Session): Boolean {
    @Language("PostgreSQL")
    val query = "SELECT COUNT(1) as antall FROM arbeidstabell"
    val antall = session.run(queryOf(query).map { it.long("antall") }.asSingle) ?: 0
    return antall > 0
}

private fun klargjørEllerVentPåTilgjengeligArbeid(session: Session, fyllArbeidstabell: () -> Unit) {
    if (fåLås(session)) {
        if (arbeidFinnes(session)) return
        return fyllArbeidstabell()
    }

    log.info("Venter på at arbeid skal bli tilgjengelig")
    while (!arbeidFinnes(session)) {
        log.info("Arbeid finnes ikke ennå, venter litt")
        runBlocking { delay(250) }
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

private fun testTilkoblinger(vararg config: HikariConfig) {
    log.info("Tester datasourcer")
    config.forEach {
        HikariDataSource(it).use {
            log.info("${it.poolName} OK!")
        }
    }
}

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